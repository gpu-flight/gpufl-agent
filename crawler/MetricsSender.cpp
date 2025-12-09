#include "MetricsSender.h"
#include "Utils.h"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <winhttp.h>
#pragma comment(lib, "winhttp.lib")
#endif

#include <fstream>
#include <iostream>
#include <algorithm>
#include <cstdint>
#include <thread>
#include <chrono>

using util::toWide;

namespace {
    void logLastError(const char* where) {
        DWORD err = GetLastError();
        LPSTR msgBuf = nullptr;
        DWORD size = FormatMessageA(
            FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
            nullptr, err, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&msgBuf, 0, NULL);
        if (size && msgBuf) {
            std::cerr << where << " GetLastError=" << err << " Message=" << msgBuf;
            LocalFree(msgBuf);
        } else {
            std::cerr << where << " GetLastError=" << err << std::endl;
        }
    }

class HttpSender final : public IMetricsSender {
public:
    HttpSender(std::string backendBase, std::string apiKey)
        : backendBase_(std::move(backendBase)), apiKey_(std::move(apiKey)) {
        if (!backendBase_.empty() && backendBase_.back() == '/') backendBase_.pop_back();
    }

    void send(const std::string& json) override {
        using namespace std::chrono;
        const std::string url = backendBase_ + "/metrics";
        std::wstring wurl = toWide(url);

        // Exponential backoff (cap at 30 seconds). Start at 200ms.
        milliseconds delay = milliseconds(200);
        constexpr milliseconds maxDelay = seconds(30);

        int attempts = 0;
        const int maxAttempts = 10; // prevents infinite blocking on shutdown
        for (; attempts < maxAttempts; ++attempts) {
            bool shouldRetry = false;
            bool fatal = false; // non-retryable
            bool success = false;

            URL_COMPONENTS uc{};
            memset(&uc, 0, sizeof(uc));
            uc.dwStructSize = sizeof(uc);
            uc.dwHostNameLength = -1;
            uc.dwUrlPathLength = -1;
            uc.dwSchemeLength = -1;

            if (!WinHttpCrackUrl(wurl.c_str(), 0, 0, &uc)) {
                std::cerr << "[HTTP] Failed to parse URL: " << url << std::endl;
                logLastError("WinHttpCrackUrl (HTTP)");
                // Bad URL is fatal; don't retry.
                fatal = true;
            } else {
                const bool secure = (uc.nScheme == INTERNET_SCHEME_HTTPS);
                const std::wstring host(uc.lpszHostName, uc.dwHostNameLength);
                const std::wstring path = uc.dwUrlPathLength > 0 ? std::wstring(uc.lpszUrlPath, uc.dwUrlPathLength) : L"/";
                const INTERNET_PORT port = uc.nPort ? uc.nPort : (secure ? 443 : 80);

                HINTERNET hSession = WinHttpOpen(L"crawler/1.0",
                                                 WINHTTP_ACCESS_TYPE_NO_PROXY,
                                                 WINHTTP_NO_PROXY_NAME,
                                                 WINHTTP_NO_PROXY_BYPASS, 0);
                if (!hSession) { logLastError("WinHttpOpen (HTTP)"); shouldRetry = true; }
                else {
                    if (HINTERNET hConnect = WinHttpConnect(hSession, host.c_str(), port, 0); !hConnect) {
                        logLastError("WinHttpConnect (HTTP)");
                        shouldRetry = true;
                        WinHttpCloseHandle(hSession);
                    } else {
                        HINTERNET hRequest = WinHttpOpenRequest(hConnect, L"POST", path.c_str(), nullptr,
                                                                WINHTTP_NO_REFERER, WINHTTP_DEFAULT_ACCEPT_TYPES,
                                                                secure ? WINHTTP_FLAG_SECURE : 0);
                        if (!hRequest) {
                            logLastError("WinHttpOpenRequest (HTTP)");
                            shouldRetry = true;
                            WinHttpCloseHandle(hConnect);
                            WinHttpCloseHandle(hSession);
                        } else {
                            std::wstring headers = L"Content-Type: application/json\r\n";
                            if (!apiKey_.empty()) {
                                headers += L"X-API-Key: " + toWide(apiKey_) + L"\r\n";
                            }

                            if (!WinHttpSendRequest(hRequest,
                                                    headers.c_str(), (DWORD)-1L,
                                                    (LPVOID)json.data(), (DWORD)json.size(), (DWORD)json.size(), 0)) {
                                std::cerr << "[HTTP] WinHttpSendRequest failed" << std::endl;
                                logLastError("WinHttpSendRequest (HTTP)");
                                shouldRetry = true;
                            } else if (!WinHttpReceiveResponse(hRequest, nullptr)) {
                                std::cerr << "[HTTP] WinHttpReceiveResponse failed" << std::endl;
                                logLastError("WinHttpReceiveResponse (HTTP)");
                                shouldRetry = true;
                            } else {
                                DWORD statusCode = 0;
                                DWORD statusSize = sizeof(statusCode);
                                if (WinHttpQueryHeaders(hRequest,
                                                        WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER,
                                                        WINHTTP_HEADER_NAME_BY_INDEX,
                                                        &statusCode, &statusSize, WINHTTP_NO_HEADER_INDEX)) {
                                    if (statusCode == 401) {
                                        std::cerr << "Unauthorized (401). Your API key may be invalid or inactive. Rotate in Account page and run again with --set-key." << std::endl;
                                        ExitProcess(1);
                                    } else if (statusCode >= 500) {
                                        std::cerr << "[HTTP] Server error " << statusCode << ". Will retry with backoff." << std::endl;
                                        shouldRetry = true;
                                    } else {
                                        // Success or non-retryable client error
                                        if (statusCode >= 200 && statusCode < 300) {
                                            success = true;
                                        } else {
                                            // Client error other than 401: do not retry
                                            fatal = true;
                                        }
                                    }
                                } else {
                                    // If we cannot read headers, treat as retryable
                                    shouldRetry = true;
                                }
                            }

                            WinHttpCloseHandle(hRequest);
                            WinHttpCloseHandle(hConnect);
                            WinHttpCloseHandle(hSession);
                        }
                    }
                }
            }

            if (success || fatal) {
                return; // either sent OK or decided not to retry
            }

            if (!shouldRetry) {
                return; // nothing else to do
            }

            // Sleep for backoff interval, then increase with cap at 30s.
            std::this_thread::sleep_for(delay);
            delay = min(delay * 2, maxDelay);
        }
        // Gave up after retries
        return;
    }

private:
    std::string backendBase_;
    std::string apiKey_;
};

} // namespace

std::unique_ptr<IMetricsSender> makeSender(Channel ch, const std::string& backendBase, const std::string& apiKey) {
    (void)ch; // Currently we only use HTTP posting to /metrics
    return std::make_unique<HttpSender>(backendBase, apiKey);
}
