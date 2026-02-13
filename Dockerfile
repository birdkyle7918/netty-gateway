# 第一阶段：编译 (使用 Maven + JDK 21)
FROM maven:3.9.6-eclipse-temurin-21-alpine AS build
WORKDIR /app

# 利用 Docker 层缓存：先只复制 pom.xml 下载依赖
COPY pom.xml .
RUN mvn dependency:go-offline

# 复制源码并编译
COPY src ./src
RUN mvn clean package -DskipTests

# 第二阶段：运行 (仅需 JRE)
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# 修正语法错误：--from=build，且目标文件名不要带斜杠
# 建议：如果 target 目录下有多个 jar（如 sources-jar），请指定具体的 jar 名称
COPY --from=build /app/target/*.jar app.jar

# 暴露端口
EXPOSE 8080 8081

# 启动命令
# 建议加上 -XX:+UseG1GC (JDK 21 默认即是) 和容器感知参数
ENTRYPOINT ["java", "-Xmx4g", "-Xms4g", "-XX:+UseContainerSupport", "-jar", "app.jar"]