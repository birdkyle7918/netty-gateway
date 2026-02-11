# 第一阶段：编译 (使用 Maven + JDK 21)
FROM maven:3.9.6-eclipse-temurin-21-alpine AS build
WORKDIR /app
# 复制 pom.xml 和源码
COPY pom.xml .
COPY src ./src
# 执行编译打包，跳过测试
RUN mvn clean package -DskipTests

# 第二阶段：运行 (仅需 JRE)
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
# 从编译阶段拷贝 jar 包
COPY --from:build /app/target/*.jar app.jar/

# 暴露端口：8080 (Netty TCP), 8081 (Spring Boot HTTP)
EXPOSE 8080 8081

# 启动命令：优化内存配置，适合 8GB 内存的机器
ENTRYPOINT ["java", "-Xmx2g", "-Xms2g", "-jar", "app.jar"]