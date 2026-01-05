#include "BlackBoxHttpServer.hpp"
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char* argv[]) {
    try {
        std::string host = "0.0.0.0";
        int port = 8080;
        std::string dataDir = "data";
        if (argc > 1 && std::string(argv[1]) == "--migrate") {
            std::string snapPath;
            if (argc > 2) snapPath = argv[2];
            minielastic::BlackBox db(dataDir);
            auto report = db.migrateCompatibility(snapPath);
            std::cout << report.dump(2) << std::endl;
            return 0;
        }
        if (argc > 1 && std::string(argv[1]) == "--ship") {
            minielastic::BlackBox db(dataDir);
            auto plan = db.shippingPlan();
            std::cout << plan.dump(2) << std::endl;
            return 0;
        }
        BlackBoxHttpServer app(host, port, dataDir);
        std::cout << "Starting server...\n";
        app.run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    } catch (...) {
        std::cerr << "Fatal unknown error\n";
        return 1;
    }
    return 0;
}
