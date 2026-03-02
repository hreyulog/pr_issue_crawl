import csv
import json
import re
import requests
from pathlib import Path
import pandas as pd
import time
GITHUB_API = "https://api.github.com"
CSV_FILE = "kafka_issue_pr_mapping_merged.csv"        # 你的 CSV 文件
OUTPUT_JSONL = "pr_info.jsonl"    # 输出 JSON 文件
TOKEN = "github_pat_zzz"  # 没有可设为 None
ISSUE_CSV="./kafka_jira_issues.csv"
def parse_issue(issue_path):
    issue_dict={}
    df = pd.read_csv(issue_path, header=0)
    for line in df.values:
        dic = {}
        for item, data in zip(df, line.tolist()):
            dic[item] = data
        issue_dict[dic['key']]=dic
        # print(dic)
        
    return issue_dict

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

def parse_pr_url(pr_url: str):
    """从 PR URL 中解析 owner, repo, pull_number"""
    pattern = r"github\.com/([^/]+)/([^/]+)/pull/(\d+)"
    match = re.search(pattern, pr_url)
    if not match:
        raise ValueError(f"不是合法的 GitHub PR 链接: {pr_url}")
    owner, repo, pull_number = match.groups()
    return owner, repo, int(pull_number)
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

def get_pr_info(pr_url: str):
    owner, repo, pull_number = parse_pr_url(pr_url)
    headers = {"Accept": "application/vnd.github+json"}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    
    # 获取 PR 基本信息
    pr_api = f"{GITHUB_API}/repos/{owner}/{repo}/pulls/{pull_number}"
    resp = requests.get(pr_api, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    
    # 获取 PR diff 内容
    diff_url = data.get("diff_url")
    diff_content = fetch_diff(diff_url) if diff_url else ""
    files=parse_diff(diff_content)
    pr_info = {
        "number": pull_number,
        "url": pr_url,
        "diff": diff_content,
        "title": data.get("title"),
        "body": data.get("body"),
        "files": files
    }
    # print(pr_info)
    return pr_info

# def main():
#     results = {}
#     csv_path = Path(CSV_FILE)
#     issue_dict=parse_issue(ISSUE_CSV)
#     if not csv_path.exists():
#         print(f"CSV 文件不存在: {CSV_FILE}")
#         return
#     i=0
#     with open(csv_path, newline="", encoding="utf-8") as f:
#         reader = csv.DictReader(f)
#         for row in reader:
#             issue_key = row["issue_key"]
#             results[issue_key]={}
#             pr_urls = eval(row["pr_url"])
#             if i>10:
#                 continue
#             results[issue_key]["issue"] = issue_dict[issue_key]
#             results[issue_key]["prs"] = [] 
#             # print(results)
#             for pr_url in pr_urls:
#                 # print(results[issue_key]['prs'])
#                 pr_info = get_pr_info(pr_url)
#                 results[issue_key]['prs'].append(pr_info)
#                 print(f"[OK] {issue_key} -> PR #{pr_info['number']}")
#             i+=1 


#     # 保存为 JSON
#     with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
#         json.dump(results, f, ensure_ascii=False, indent=2)
#     print(f"保存完成: {OUTPUT_JSON}")


def main():
    csv_path = Path(CSV_FILE)
    issue_dict = parse_issue(ISSUE_CSV)

    if not csv_path.exists():
        print(f"CSV 文件不存在: {CSV_FILE}")
        return

    i = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # jsonl 用追加模式
        with open(OUTPUT_JSONL, "a", encoding="utf-8") as out:
            for row in reader:
                # if i > 10:
                #     break

                issue_key = row["issue_key"]
                pr_urls = eval(row["pr_url"])

                record = {
                    "issue_key": issue_key,
                    "issue": issue_dict.get(issue_key),
                    "prs": []
                }

                for pr_url in pr_urls:
                    pr_info = get_pr_info(pr_url)
                    record["prs"].append(pr_info)
                    print(f"[OK] {issue_key} -> PR #{pr_info['number']}")

                # 一行一个 JSON
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                out.flush()  # 可选：防止中途崩溃丢数据

                # i += 1

    print(f"保存完成: {OUTPUT_JSONL}")
            # i += 1
if __name__ == "__main__":
    main()
