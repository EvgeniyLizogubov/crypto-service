package io.github.evgeniylizogubov.cryptoservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "spring.docker.compose.enabled=false",
        "spring.liquibase.enabled=false",
        "spring.r2dbc.url=r2dbc:postgresql://localhost:5432/test",
        "spring.r2dbc.username=test",
        "spring.r2dbc.password=test",
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6379",
        "crypto.providers.mock.enabled=true"
})
class CryptoServiceApplicationTests {

    @Test
    void contextLoads() {
    }

}
