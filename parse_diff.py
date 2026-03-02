import json
import requests
import time

import requests
import time

def fetch_diff(url, retries=5, delay=10):
    """
    获取 diff 内容，如果失败则重试指定次数。
    
    参数:
        url (str): 请求的 URL
        retries (int): 最大重试次数，默认 3 次
        delay (float): 每次重试之间的等待时间（秒），默认 1 秒

    返回:
        str: 成功返回内容，失败返回空字符串
    """
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.text
            else:
                print(f"请求失败 {url} 状态码: {response.status_code} (尝试 {attempt}/{retries})")
        except Exception as e:
            print(f"请求异常 {url}: {e} (尝试 {attempt}/{retries})")
        
        if attempt < retries:
            time.sleep(delay)  # 等待再重试
    
    print(f"最终失败，无法获取 {url}")
    return ""

def parse_diff(diff_text):
    """
    解析 diff 内容，返回每个文件的 added、deleted、diff
    """
    files = []
    current_file = None

    for line in diff_text.splitlines():
        # 新文件 diff
        if line.startswith("diff --git"):
            if current_file:
                files.append(current_file)
            parts = line.split(" b/")
            filename = parts[1] if len(parts) > 1 else ""
            current_file = {"file": filename, "added": 0, "deleted": 0, "diff": ""}
            current_file["diff"] += line + "\n"
            continue

        if current_file:
            if line.startswith("diff --git"):
                # 已在循环开头处理
                continue
            # 统计新增/删除行
            if line.startswith("+") and not line.startswith("+++"):
                current_file["added"] += 1
            elif line.startswith("-") and not line.startswith("---"):
                current_file["deleted"] += 1
            current_file["diff"] += line + "\n"

    # 添加最后一个文件
    if current_file:
        files.append(current_file)

    return files

def parse_diff(diff_text):
    """
    解析 diff 内容，返回每个文件的 added、deleted、diff
    """
    files = []
    current_file = None

    for line in diff_text.splitlines():
        # 新文件 diff
        if line.startswith("diff --git"):
            if current_file:
                files.append(current_file)
            parts = line.split(" b/")
            filename = parts[1] if len(parts) > 1 else ""
            current_file = {"file": filename, "added": 0, "deleted": 0, "diff": ""}
            current_file["diff"] += line + "\n"
            continue

        if current_file:
            if line.startswith("diff --git"):
                # 已在循环开头处理
                continue
            # 统计新增/删除行
            if line.startswith("+") and not line.startswith("+++"):
                current_file["added"] += 1
            elif line.startswith("-") and not line.startswith("---"):
                current_file["deleted"] += 1
            current_file["diff"] += line + "\n"

    # 添加最后一个文件
    if current_file:
        files.append(current_file)

    return files


def main():
    # 1. 读取原始 JSON 文件
    try:
        with open("pr_info.json", "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("错误：未找到 raw_issues.json 文件")
        return
    except json.JSONDecodeError:
        print("错误：JSON 文件解析失败")
        return

    # 2. 提取字段并抓取 diff
    simplified_issues = []
    for item in data:
        item=data[item]

        diff_url = item.get("diff_url", "")
        # diff_content=item.get("diff","")
        diff_content = fetch_diff(diff_url) if diff_url else ""
        files=parse_diff(diff_content)
        simplified_issues.append({
            "number": item.get("number"),
            "url": item.get("url"),
            "diff": diff_content,
            "title": item.get("title"),
            "body": item.get("body"),
            "files": files
        })

        # 为了防止请求太快被封，可加短暂延迟

    # 3. 保存为 JSON 文件
    with open("issues_with_diff.json", "w", encoding="utf-8") as f:
        json.dump(simplified_issues, f, ensure_ascii=False, indent=2)

    print("完成！已保存为 issues_with_diff.json")

if __name__ == "__main__":
    main()
