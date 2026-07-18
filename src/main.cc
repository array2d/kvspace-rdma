// kvspace-rdma server entry point.
#include "server/server.h"
#include <cstdlib>
#include <iostream>
#include <csignal>

using namespace kvspace::server;

namespace {
    Server* g_server = nullptr;
    void sig_handler(int) {
        if (g_server) g_server->stop();
        std::exit(0);
    }
}

int main(int argc, char* argv[]) {
    Server::Config cfg;
    cfg.listen_addr = "0.0.0.0:9000";
    cfg.num_shards  = 1;

    for (int i = 1; i < argc; i++) {
        std::string arg(argv[i]);
        if (arg == "-l" && i + 1 < argc) cfg.listen_addr = argv[++i];
        else if (arg == "-n" && i + 1 < argc) cfg.num_shards = std::stoi(argv[++i]);
        else if (arg == "-h" || arg == "--help") {
            std::cout << "kvspace-rdma server\n"
                      << "  -l addr    listen address (default 0.0.0.0:9000)\n"
                      << "  -n N       number of shards (default 1)\n"
                      << "  -h         help\n";
            return 0;
        }
    }

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    Server server(cfg);
    g_server = &server;

    std::cout << "kvspace-rdma listening on " << cfg.listen_addr
              << " (shards=" << cfg.num_shards
              << ", slots/shard=" << cfg.slots_per_shard << ")\n";

    server.start();

    // wait for signal
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    int sig;
    sigwait(&set, &sig);

    server.stop();
    std::cout << "\nprocessed " << server.total_processed() << " requests\n";
    return 0;
}
