"""Generate/check version and acceptance projections from one product model."""
import argparse
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
MODEL = ROOT / "metadata/product.json"


def read_model():
    model = json.loads(MODEL.read_text(encoding="utf-8"))
    if model["modelVersion"] != 1 or len(model["goals"]) != 69:
        raise ValueError("Unsupported product model or changed goal inventory.")
    if (model["product"]["abiVersion"] != model["product"]["protocolVersion"]
            or not 0 < model["product"]["abiVersion"] <= 4294967295):
        raise ValueError("ABI and all protocol epochs must be one positive uint32 version.")
    for key in ("goals", "gates", "remediations"):
        identifiers = [item["id"] for item in model[key]]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("Duplicate identifiers in " + key)
    return model


def encoded(value):
    return json.dumps(value, ensure_ascii=False, indent=2) + "\n"


def projections(model):
    product = model["product"]
    version, abi, protocol = product["sdkVersion"], product["abiVersion"], product["protocolVersion"]
    marker = "metadata/product.json"
    header = "/* Generated from metadata/product.json. */\n#ifndef UD_VERSION_H\n#define UD_VERSION_H\n"
    header += '#define UD_SDK_VERSION "' + version + '"\n'
    header += "#define UD_ABI_VERSION " + str(abi) + "u\n"
    header += '#define UD_PROTOCOL_VERSION_TEXT "' + str(protocol) + '"\n'
    for name in ("CONFIG", "PLAN", "CHECKPOINT", "WORKER", "WIRE", "PLUGIN_ABI", "PACKAGE_VERSION"):
        macro = "UD_" + name if name in ("PLUGIN_ABI", "PACKAGE_VERSION") else "UD_" + name + "_VERSION"
        header += "#define " + macro + " " + str(protocol) + "u\n"
    header += "#endif\n"
    facts = {"generatedFrom": marker, "sdkVersion": version, "abi": abi,
             "scope": model["scope"], "baseline": model["baseline"], "candidate": model["candidate"],
             "remoteCI": model["remoteCI"], "artifacts": model["artifacts"]}
    passed = [gate for gate in model["gates"] if gate["status"] == "Passed"]
    progress = dict(facts, requiredGates=len(model["gates"]), passedGates=len(passed),
                    remediations=model["remediations"])
    status = dict(facts, components="docs/product/component-catalog.md",
                  remediations=model["remediations"], limitations=model["limitations"])
    goals = {"generatedFrom": marker, "goalVersion": protocol,
             "completionRule": "Current core/infrastructure gates are required; extensions retain their actual status.",
             "automaticMergeOrReleaseAuthorized": False, "items": model["goals"]}
    gates = {"generatedFrom": marker, "version": protocol, "sdkVersion": version, "abi": abi,
             "sourceFingerprint": model["candidate"]["sourceFingerprint"],
             "linux": model["scope"]["linux"], "gates": model["gates"],
             "delivery": model["artifacts"]}
    catalog = "# 当前组件清单与 SSIS 映射\n\n<!-- Generated from metadata/product.json. -->\n\n"
    catalog += "SDK " + version + "，唯一 ABI/协议 " + str(abi) + "。本轮范围：统一架构与原生核心。验收事实见 metadata/product.json；Linux 按用户决定免测。\n\n"
    catalog += "| SSIS 职责 | 实际入口/ID | 实现与边界 |\n|---|---|---|\n"
    for row in model["componentCatalog"]:
        catalog += "| " + row["ssis"] + " | " + row["entries"] + " | " + row["contract"] + " |\n"
    catalog += "\n未完成扩展：" + model["deferredComponents"] + "。\n\n"
    catalog += "Arrow C Data Interface 与原生内存数据模型不等同于 Parquet driver。Microsoft .dtsx/COM/SSISDB/MSDTC 专属兼容不承诺；没有占位注册或旧 generic operation 路径。\n"
    progress_md = "# 当前实施与验收进度\n\n<!-- Generated from metadata/product.json. -->\n\n"
    progress_md += "SDK " + version + " / ABI " + str(abi) + "；" + model["candidate"]["state"] + "。\n\n"
    progress_md += "门禁通过 " + str(len(passed)) + "/" + str(len(model["gates"])) + "；Linux 本轮免测；CI 账单阻塞单独记录。\n\n"
    progress_md += "| 整改 | 当前状态 |\n|---|---|\n"
    for item in model["remediations"]:
        progress_md += "| " + item["id"] + " | " + item["state"] + " / " + item["verification"] + " |\n"
    return {
        "VERSION": version + "\n",
        "bindings/java/pom.xml": (ROOT / "metadata/java-pom.xml.in").read_text(encoding="utf-8").format(sdk_version=version),
        ".github/workflows/ci.yml": (ROOT / "metadata/ci.yml.in").read_text(encoding="utf-8").replace("@UD_SDK_VERSION@", version),
        "include/ud/version.h": header,
        "metadata/ProductVersion.props": '<!-- Generated from metadata/product.json. -->\n'
            '<Project><PropertyGroup><UDSdkVersion>' + version + '</UDSdkVersion></PropertyGroup></Project>\n',
        "bindings/dotnet/UniversalData/ProductVersion.cs":
            "// Generated from metadata/product.json.\nnamespace UniversalData;\n"
            "internal static class ProductVersion\n{\n    internal const uint Abi = " + str(abi) +
            ";\n}\n",
        "bindings/java/src/main/java/org/universaldata/ProductVersion.java":
            "// Generated from metadata/product.json.\npackage org.universaldata;\n"
            "public final class ProductVersion {\n    private ProductVersion() {}\n"
            "    public static final int ABI = " + str(abi) + ";\n"
            "    public static final int PROTOCOL = " + str(protocol) + ";\n"
            '    public static final String SDK = "' + version + '";\n'
            '    static void requireNative() {\n        System.loadLibrary("ud_java");\n'
            '        if (nativeAbi() != ABI) throw new UniversalDataException(10, "Native ABI differs from SDK");\n    }\n'
            '    private static native int nativeAbi();\n}\n',
        "docs/acceptance/progress.json": encoded(progress),
        "docs/acceptance/ssis-status.json": encoded(status),
        "docs/acceptance/completion-goal.json": encoded(goals),
        "docs/acceptance/gates.json": encoded(gates),
        "docs/acceptance/progress.md": progress_md,
        "docs/product/component-catalog.md": catalog,
    }


def generate(check=False):
    errors = []
    for name, text in projections(read_model()).items():
        path = ROOT / name
        if check:
            if not path.is_file() or path.read_bytes() != text.encode("utf-8"):
                errors.append(name)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8", newline="\n")
    return errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("generate", "check"))
    arguments = parser.parse_args()
    differences = generate(arguments.action == "check")
    if differences:
        print("Product projections differ from authority: " + ", ".join(differences))
        sys.exit(1)
    print("Product projections " + ("checked" if arguments.action == "check" else "generated"))
