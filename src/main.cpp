#include "BlackBoxHttpServer.hpp"
#include <iostream>

int main() {
    try {
        BlackBoxHttpServer app("0.0.0.0", 8080, "data");
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
