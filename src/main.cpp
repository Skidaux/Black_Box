#include "MiniElasticHttpServer.hpp"

int main() {
    MiniElasticHttpServer app("0.0.0.0", 8080);
    app.run();
    return 0;
}
