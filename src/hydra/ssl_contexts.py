import ssl


def extract_ssl_context_info(context: ssl.SSLContext) -> dict:
    return {
        "protocol": context.protocol,
        "verify_mode": int(context.verify_mode),
        "check_hostname": context.check_hostname,
        "options": int(context.options),
        "verify_flags": int(context.verify_flags),
    }


def rebuild_ssl_context(info: dict) -> ssl.SSLContext:
    # 1. Initialize a new context with the original protocol
    context = ssl.SSLContext(info["protocol"])

    # 2. Restore options and flags using bitwise OR
    context.options |= info["options"]
    context.verify_flags |= info["verify_flags"]

    # 3. Restore verification behavior
    context.verify_mode = ssl.VerifyMode(info["verify_mode"])
    context.check_hostname = info["check_hostname"]

    return context
