FROM mcr.microsoft.com/dotnet/sdk:8.0-bookworm-slim AS build
RUN apt-get update && apt-get install -y git
WORKDIR /src
RUN git clone --depth 1 https://github.com/oliverw/miningcore.git .
RUN dotnet publish $(find . -name "Miningcore.csproj" | head -n 1) -c Release -o /app/build

FROM mcr.microsoft.com/dotnet/runtime:8.0-bookworm-slim
WORKDIR /app
COPY --from=build /app/build /app/build
ENTRYPOINT ["/app/build/Miningcore", "-c", "/app/config.json"]
