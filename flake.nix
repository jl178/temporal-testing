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
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            nodejs_20
            go
            python312
            dotnet-sdk_8
            awscli2
            temporal-cli
            jq
          ];

          shellHook = ''
            export DOTNET_CLI_TELEMETRY_OPTOUT=1
            export DOTNET_ROOT="${pkgs.dotnet-sdk_8}/share/dotnet"
            echo "temporal-testing dev shell: node $(node --version), go $(go version | cut -d' ' -f3), python $(python3 --version | cut -d' ' -f2), dotnet $(dotnet --version)"
          '';
        };
      });
}
