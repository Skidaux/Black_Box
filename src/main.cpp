#include "BlackBoxHttpServer.hpp"

int main() {
    BlackBoxHttpServer app("0.0.0.0", 8080, "data");
    app.run();
    return 0;
}
