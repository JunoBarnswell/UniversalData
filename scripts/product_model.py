"""Generate/check version and acceptance projections from one product model."""
import argparse
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
MODEL = ROOT / "metadata/product.json"


def read_model():
    model = json.loads(MODEL.read_text(encoding="utf-8"))
    if model["modelVersion"] != 2 or len(model["goals"]) != 52:
        raise ValueError("Unsupported product model or changed goal inventory.")
    if {item["id"] for item in model["goals"]} != {f"ETL-{n:03d}" for n in range(1, 53)}:
        raise ValueError("All 52 audit IDs must be tracked in the existing goal inventory.")
    if {item["id"] for item in model["goals"] if not item["requiredThisDelivery"]} != {"ETL-039", "ETL-040"}:
        raise ValueError("Only DQS and SSAS Data Mining are NotApplicable.")
    if (model["product"]["abiVersion"] != model["product"]["protocolVersion"]
            or not 0 < model["product"]["abiVersion"] <= 4294967295):
        raise ValueError("ABI and all protocol epochs must be one positive uint32 version.")
    for key in ("goals", "gates"):
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
    history = model["history"]
    native_types = re.findall(r"UD_(\w+)\s*=\s*(\d+)",
        (ROOT / "include/ud/data.h").read_text().split("} ud_decimal128;", 1)[0])
    type_names = {"BOOL":"Bool", "INT64":"Int64", "FLOAT64":"Float64", "UTF8":"Utf8",
        "DECIMAL128":"Decimal128", "DATE32":"Date32", "TIME64":"Time64", "TIMESTAMP":"Timestamp",
        "BINARY":"Binary", "UINT64":"UInt64", "DURATION":"Duration", "INTERVAL":"Interval",
        "DATETIME":"DateTime", "STRUCT":"Struct", "LIST":"List", "MAP":"Map"}
    cs_types = "// Generated from include/ud/data.h; Native owns value semantics.\nnamespace UniversalData;\npublic enum DataType\n{\n"
    cs_types += ",\n".join("    " + type_names[name] + " = " + number for name,number in native_types) + "\n}\n"
    cs_types += "public readonly record struct Duration(long Microseconds);\npublic readonly record struct Interval(int Months, int Days, long Microseconds);\n"
    java_types = "// Generated from include/ud/data.h; Native owns value semantics.\npackage org.universaldata;\npublic enum DataType {\n"
    java_types += ",\n".join("    " + name + "(" + number + ")" for name,number in native_types)
    java_types += ";\n    public final int nativeCode;\n    DataType(int code) { nativeCode = code; }\n}\n"
    header = "/* Generated from metadata/product.json. */\n#ifndef UD_VERSION_H\n#define UD_VERSION_H\n"
    header += '#define UD_SDK_VERSION "' + version + '"\n'
    header += "#define UD_ABI_VERSION " + str(abi) + "u\n"
    header += '#define UD_PROTOCOL_VERSION_TEXT "' + str(protocol) + '"\n'
    for name in ("CONFIG", "PLAN", "CHECKPOINT", "WORKER", "WIRE", "PLUGIN_ABI", "PACKAGE_VERSION"):
        macro = "UD_" + name if name in ("PLUGIN_ABI", "PACKAGE_VERSION") else "UD_" + name + "_VERSION"
        header += "#define " + macro + " " + str(protocol) + "u\n"
    header += "#define UD_CHECKPOINT_FORMAT_REVISION " + str(product["checkpointRevision"]) + "u\n"
    header += "#define UD_DATA_LAYOUT_REVISION " + str(product["dataLayoutRevision"]) + "u\n"
    header += "#define UD_CATALOG_FORMAT_REVISION " + str(product["catalogRevision"]) + "u\n"
    header += "#define UD_WORKER_FORMAT_REVISION " + str(product["workerRevision"]) + "u\n"
    header += "#endif\n"
    facts = {"generatedFrom": marker, "sdkVersion": version, "abi": abi,
             "scope": model["scope"], "baseline": model["baseline"], "candidate": model["candidate"],
             "remoteCI": model["remoteCI"], "artifacts": model["artifacts"]}
    passed = [gate for gate in model["gates"] if gate["status"] == "Passed"]
    progress = dict(facts, requiredGates=sum(gate["status"] != "Exempt" for gate in model["gates"]), passedGates=len(passed),
                    items=model["goals"], designReviews=model["productAlignment"]["designReviews"])
    status = dict(facts, components="docs/product/component-catalog.md",
                  items=model["goals"], limitations=model["limitations"])
    strict = dict(generatedFrom=marker, sdkVersion=version, abi=abi,
                  **model["productAlignment"].get("strictParity", {}))
    strict_md = "# SSIS strict parity evidence\n\nGenerated from `metadata/product.json`; this is a projection of the existing product model.\n\n"
    strict_md += "Native checks establish the stated invariants only. They do not establish Microsoft equivalence. No item is GoldenVerified without a real Windows SSIS capture.\n\n"
    strict_md += "| ID | Requirement | Current code | Native evidence | Remaining code / reference proof |\n|---|---|---|---|---|\n"
    for row in strict.get("matrix", []):
        cells = [row["id"], row["name"], ", ".join(row["source"]),
                 ", ".join(row.get("nativeChecks", [])), row.get("remaining", "")]
        strict_md += "| " + " | ".join(str(cell).replace("|", "\\|").replace("\n", " ") for cell in cells) + " |\n"
    goals = {"generatedFrom": marker, "goalVersion": protocol,
             "completionRule": "All 50 applicable items require current source and behavior evidence; ETL-039/040 are NotApplicable. Missing environments are Blocked.",
             "automaticMergeOrReleaseAuthorized": False, "items": model["goals"]}
    gates = {"generatedFrom": marker, "version": protocol, "sdkVersion": version, "abi": abi,
             "sourceFingerprint": model["candidate"]["sourceFingerprint"],
             "linux": model["scope"]["linux"], "windows": model["scope"].get("windows"), "gates": model["gates"],
             "delivery": model["artifacts"]}
    catalog = "# 当前组件清单与 SSIS 映射\n\n<!-- Generated from metadata/product.json. -->\n\n"
    catalog += "SDK " + version + "，唯一 ABI/协议 " + str(abi) + "。Linux 与 Windows 均须验收。下列目录是 main 基线已有组件的入口与边界，不能代替 2.0 当前验收；50 项适用整改状态见 metadata/product.json 的 goals。\n\n"
    catalog += "| SSIS 职责 | 实际入口/ID | 实现与边界 |\n|---|---|---|\n"
    for row in model["componentCatalog"]:
        catalog += "| " + row["ssis"] + " | " + row["entries"] + " | " + row["contract"] + " |\n"
    catalog += "\n未完成扩展：" + model["deferredComponents"] + "。\n\n"
    catalog += "Arrow C Data Interface 与原生内存数据模型不等同于 Parquet driver。Microsoft .dtsx/COM/SSISDB/MSDTC 专属兼容不承诺；没有占位注册或旧 generic operation 路径。\n"
    progress_md = "# 当前实施与验收进度\n\n<!-- Generated from metadata/product.json. -->\n\n"
    progress_md += "SDK " + version + " / ABI " + str(abi) + "；" + model["candidate"]["state"] + "。\n\n"
    progress_md += "门禁通过 " + str(len(passed)) + "/" + str(len(model["gates"])) + "；Linux 与 Windows 均必验；历史 ABI 的结果不计入当前版本。\n\n"
    progress_md += "| 整改 | 当前状态 |\n|---|---|\n"
    for item in model["goals"]:
        progress_md += "| " + item["id"] + " " + item["name"] + " | " + item["code"] + " / " + item["verification"] + " |\n"
    progress_md += "\n三轮有界设计复核：\n\n"
    for review in model["productAlignment"]["designReviews"]:
        progress_md += str(review["round"]) + ". " + review["scope"] + "（" + review["result"] + "）：" + "；".join(review["decisions"]) + "\n"
    trace = "# 四引擎需求与验收追踪\n\n<!-- Generated from metadata/product.json. -->\n\n"
    trace += "历史 ABI 9 工作项投影，保留旧证据供追溯，不计入 SDK 2.0 验收。当前 52 项审查以现有 completion-goal.json 与 metadata/product.json 为准。\n\n"
    trace += "| ID | 优先级 | 阶段 | 能力 | 代码 | 验收 | 69 项目标映射 |\n|---|---|---|---|---|---|---|\n"
    for row in history["fourEngineWorkItems"]:
        trace += "| " + row["id"] + " | " + row["priority"] + " | " + row["phase"] + " | " + row["name"] + " | " + row["state"] + " | " + row["verification"] + " | " + ", ".join(row["goals"]) + " |\n"
    next_trace = "# R1–R5 主分支整改追踪\n\n<!-- Generated from metadata/product.json. -->\n\n"
    next_trace += "历史五阶段工作项，不计入 SDK 2.0 当前验收。OBS 是历史 R5 测量要求的内部 ID。\n\n| ID | 阶段 | 能力 | 代码 | 验收 | 证据 |\n|---|---|---|---|---|---|\n"
    for row in history["nextPhaseWorkItems"]:
        next_trace += "| " + row["id"] + " | " + row["phase"] + " | " + row["name"] + " | " + row["state"] + " | " + row["verification"] + " | " + ", ".join(row["evidence"]) + " |\n"
    advanced_trace="# PR #5 历史基础与高级能力追踪\n\n<!-- Generated from metadata/product.json history. -->\n\n以下是 main 基线之前的历史事实，不计入 SDK 2.0 验收。当前 Linux 与 Windows 均须验收。\n\n| ID | 能力 | 实现状态 | 验收 |\n|---|---|---|---|\n"
    for item in history["advancedWorkItems"]:
        advanced_trace+="| "+item["id"]+" | "+item["name"]+" | "+item["state"]+" | "+item["verification"]+" |\n"
    designer_package=json.loads((ROOT/"products/designer/package.json").read_text());designer_package["version"]=version
    designer_lock=json.loads((ROOT/"products/designer/package-lock.json").read_text());designer_lock["version"]=version;designer_lock["packages"][""]["version"]=version
    studio_trace="# Studio 4 实施与验收追踪\n\n<!-- Generated from metadata/product.json studio4. -->\n\n实现、验收与可发布性分别记录；Planned 不计为已完成。\n\n| ID | 目标 | 实现 | 验收 | 限制 | 证据 |\n|---|---|---|---|---|---|\n"
    for item in model.get("studio4",{}).get("items",[]):
        studio_trace+="| "+item["id"]+" | "+item["goal"]+" | "+item["implementation"]+" | "+item["verification"]+" | "+str(item.get("blocked") or "—")+" | "+", ".join(item["evidence"])+" |\n"
    return {
        "docs/acceptance/studio4-traceability.md":studio_trace,
        "products/designer/package.json": encoded(designer_package),
        "products/designer/package-lock.json": encoded(designer_lock),
        "products/designer/src/dataTypes.ts": "// Generated from include/ud/data.h. Native owns type semantics.\nexport const DATA_TYPES = " + json.dumps([name.lower() for name, _ in native_types]) + " as const;\n",
        "bindings/dotnet/UniversalData/DataType.cs": cs_types,
        "bindings/java/src/main/java/org/universaldata/DataType.java": java_types,
        "bindings/java/src/main/java/org/universaldata/Duration.java": "package org.universaldata;\n/** Signed native microseconds, without narrowing to nanoseconds. */\npublic record Duration(long microseconds) {}\n",
        "bindings/java/src/main/java/org/universaldata/Interval.java": "package org.universaldata;\n/** Independent native calendar components. */\npublic record Interval(int months, int days, long microseconds) {}\n",
        "docs/acceptance/pr5-advanced-traceability.md":advanced_trace,
        "docs/acceptance/etl-next-phase-traceability.md": next_trace,
        "docs/acceptance/etl-four-engine-traceability.md": trace,
        "VERSION": version + "\n",
        "bindings/java/pom.xml": (ROOT / "metadata/java-pom.xml.in").read_text(encoding="utf-8").format(sdk_version=version),
        ".github/workflows/ci.yml": (ROOT / "metadata/ci.yml.in").read_text(encoding="utf-8").replace("@UD_SDK_VERSION@", version),
        "include/ud/version.h": header,
        "metadata/ProductVersion.props": '<!-- Generated from metadata/product.json. -->\n'
            '<Project><PropertyGroup><UDSdkVersion>' + version + '</UDSdkVersion></PropertyGroup></Project>\n',
        "bindings/dotnet/UniversalData/ProductVersion.cs":
            "// Generated from metadata/product.json.\nnamespace UniversalData;\n"
            "internal static class ProductVersion\n{\n    internal const uint Abi = " + str(abi) +
            ";\n    internal const string Sdk = \"" + version + "\";\n}\n",
        "bindings/dotnet/UniversalData/ProductInfo.cs":
            "// Generated from metadata/product.json.\nnamespace UniversalData;\n"
            "public static class ProductInfo\n{\n    public const string Sdk = \"" + version +
            "\";\n    public const int Protocol = " + str(protocol) + ";\n    public const int WorkerRevision = " + str(product["workerRevision"]) + ";\n}\n",
        "products/designer/src/version.ts":
            "// Generated from metadata/product.json.\nexport const PRODUCT_VERSION = \"" + version +
            "\";\nexport const PROTOCOL_VERSION = " + str(protocol) + ";\n",
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
        "docs/acceptance/ssis-strict-parity.json": encoded(strict),
        "docs/acceptance/ssis-strict-parity.md": strict_md,
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
            if not path.is_file() or path.read_bytes() != text.encode("utf-8"):
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
