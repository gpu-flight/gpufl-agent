package com.gpuflight.agent;

import com.gpuflight.agent.config.ConfigLoader;
import com.gpuflight.agent.model.AgentConfig;

public class Main {
    public static void main(String[] args) throws Exception {
        AgentConfig config = ConfigLoader.load(args);
        GpuflAgent agent = new GpuflAgent(config, args, System.getenv());
        agent.start();
    }
}
