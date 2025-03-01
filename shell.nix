{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  buildInputs = [
    pkgs.python311
    pkgs.python311Packages.pip
    pkgs.python311Packages.virtualenv
    pkgs.python311Packages.flask
    pkgs.python311Packages.gunicorn
    pkgs.sqlite
    pkgs.openssl
  ];

  # Если требуется, можно задать дополнительные переменные окружения через shellHook:
  shellHook = ''
    export LD_LIBRARY_PATH=${pkgs.stdenv.cc.cc.lib}/lib
    # PYTHONPATH не требуется, так как Nix автоматически настраивает пути для установленных пакетов.
  '';
}
