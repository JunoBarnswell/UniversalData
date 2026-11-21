"""Small helpers shared by the real orders/payments/shops fixtures."""

import socket
import subprocess


def command(argv, *, timeout=60, input_text=None, env=None, check=True):
    result = subprocess.run([str(value) for value in argv], input=input_text,
                            capture_output=True, text=True, timeout=timeout, env=env)
    if check and result.returncode:
        raise RuntimeError("command failed (%s): %s\n%s\n%s" %
                           (result.returncode, " ".join(map(str, argv)),
                            result.stdout, result.stderr))
    return result


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def quote_identifier(value):
    return '"' + str(value).replace('"', '""') + '"'


def quote_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def mysql_identifier(value):
    return "`" + str(value).replace("`", "``") + "`"
