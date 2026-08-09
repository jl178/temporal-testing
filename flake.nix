{
  description = "Temporal e2e example: local Docker Compose runtime, AWS CDK infra, multi-language workflows";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true; # dotnet et al.
        };
        # Native libs required by pip wheels (numpy/pyarrow/pandas) on NixOS.
        wheelLibPath = pkgs.lib.makeLibraryPath [ pkgs.stdenv.cc.cc.lib pkgs.zlib ];
      in
      rec {
        # Task runner: `nix run .#<name>` from anywhere inside the repo.
        # Apps operate on the working tree (git toplevel), not the store copy.
        apps =
          let
            toolchain = with pkgs; [
              nodejs_20
              go
              python312
              dotnet-sdk_8
              awscli2
              temporal-cli
              jq
              git
              jdk17_headless # pyspark (dbt-spark) needs a JVM
            ];
            mkApp = name: text: {
              type = "app";
              program = pkgs.lib.getExe (pkgs.writeShellApplication {
                inherit name;
                runtimeInputs = toolchain;
                text = ''
                  cd "$(git rev-parse --show-toplevel)"
                  export DOTNET_CLI_TELEMETRY_OPTOUT=1
                  export LD_LIBRARY_PATH="${wheelLibPath}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
                  ${text}
                '';
              });
            };
          in
          {
            up = mkApp "up" ''docker compose up -d'';
            down = mkApp "down" ''docker compose down "$@"'';
            prod-up = mkApp "prod-up" ''docker compose -f docker-compose.prod.yml up -d --build'';
            prod-down = mkApp "prod-down" ''docker compose -f docker-compose.prod.yml down "$@"'';
            catalog-up = mkApp "catalog-up" ''docker compose -f docker-compose.catalog.yml up -d'';
            catalog-down = mkApp "catalog-down" ''docker compose -f docker-compose.catalog.yml down "$@"'';
            sftp-up = mkApp "sftp-up" ''docker compose up -d sftp'';
            sftp-down = mkApp "sftp-down" ''docker compose rm -sf sftp'';
            spark-up = mkApp "spark-up" ''docker compose -f docker-compose.spark.yml up -d'';
            spark-down = mkApp "spark-down" ''docker compose -f docker-compose.spark.yml down "$@"'';
            examples = mkApp "examples" ''exec scripts/validate-local.sh "$@"'';
            infra-test = mkApp "infra-test" ''
              cd infra
              [ -d node_modules ] || npm install --no-fund --no-audit --loglevel=error
              exec npx jest "$@"
            '';
            synth = mkApp "synth" ''
              cd infra
              [ -d node_modules ] || npm install --no-fund --no-audit --loglevel=error
              CDK_DEFAULT_ACCOUNT="''${CDK_DEFAULT_ACCOUNT:-111111111111}" \
                CDK_DEFAULT_REGION="''${CDK_DEFAULT_REGION:-us-east-1}" \
                exec npx cdk synth "$@"
            '';
            validate-emulator = mkApp "validate-emulator" ''exec scripts/validate-emulator.sh "$@"'';
            validate = mkApp "validate" ''
              scripts/validate-local.sh
              cd infra
              [ -d node_modules ] || npm install --no-fund --no-audit --loglevel=error
              npx jest
            '';
          };

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            nodejs_20
            go
            python312
            dotnet-sdk_8
            awscli2
            temporal-cli
            jq
            jdk17_headless # pyspark (dbt-spark) needs a JVM
          ];

          shellHook = ''
            export DOTNET_CLI_TELEMETRY_OPTOUT=1
            export DOTNET_ROOT="${pkgs.dotnet-sdk_8}/share/dotnet"
            export LD_LIBRARY_PATH="${wheelLibPath}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
            echo "temporal-testing dev shell: node $(node --version), go $(go version | cut -d' ' -f3), python $(python3 --version | cut -d' ' -f2), dotnet $(dotnet --version)"
          '';
        };
      });
}
