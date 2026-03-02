import requests
import time
import re
import json
import pandas as pd

# ======================
# Jira 配置
# ======================
JIRA_SEARCH_API = "https://issues.apache.org/jira/rest/api/2/search"

JQL = """
project = KAFKA
AND issuetype in (Bug, Improvement)
ORDER BY created ASC
"""

PAGE_SIZE = 100
SLEEP_SECONDS = 0.3

# ======================
# PR 正则（Kafka）
# ======================
PR_PATTERN = re.compile(
    r"https://github\.com/apache/kafka/pull/\d+"
)

# ======================
# 拉取 Jira Issues
# ======================
def fetch_all_issues():
    start_at = 0
    all_issues = []

    while True:
        params = {
            "jql": JQL,
            "startAt": start_at,
            "maxResults": PAGE_SIZE,
            "fields": ",".join([
                "summary",
                "status",
                "created",
                "updated",
                "description",
                "comment"
            ])
        }

        print(f"[JIRA] Fetching startAt={start_at}")
        resp = requests.get(JIRA_SEARCH_API, params=params, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        issues = data.get("issues", [])

        if not issues:
            break

        all_issues.extend(issues)
        start_at += PAGE_SIZE
        time.sleep(SLEEP_SECONDS)

    print(f"[JIRA] Total issues fetched: {len(all_issues)}")
    return all_issues


# ======================
# 从 Issue 中抽 PR
# ======================
def extract_pr_links(issue):
    fields = issue["fields"]
    links = set()

    # description
    desc = fields.get("description") or ""
    links.update(PR_PATTERN.findall(desc))

    # comments
    comments = fields.get("comment", {}).get("comments", [])
    for c in comments:
        body = c.get("body") or ""
        links.update(PR_PATTERN.findall(body))

    return list(links)


# ======================
# 建立 Issue ↔ PR 映射
# ======================
def build_issue_pr_mapping(issues):
    records = []

    for issue in issues:
        issue_key = issue["key"]
        pr_links = extract_pr_links(issue)

        if not pr_links:
            continue

        for pr in pr_links:
            records.append({
                "issue_key": issue_key,
                "pr_url": pr
            })

    print(f"[MAP] Total Issue-PR links: {len(records)}")
    return records


# ======================
# 保存结果
# ======================
def save_results(issues, mappings):
    # 原始 Jira 数据
    with open("kafka_jira_issues_raw.json", "w", encoding="utf-8") as f:
        json.dump(issues, f, ensure_ascii=False, indent=2)

    # 映射表
    df = pd.DataFrame(mappings)
    df.to_csv("kafka_issue_pr_mapping.csv", index=False)
    df.to_json(
        "kafka_issue_pr_mapping.json",
        orient="records",
        force_ascii=False,
        indent=2
    )

    print("[SAVE] Files written:")
    print("  - kafka_jira_issues_raw.json")
    print("  - kafka_issue_pr_mapping.csv")
    print("  - kafka_issue_pr_mapping.json")


# ======================
# 主流程
# ======================
def main():
    issues = fetch_all_issues()
    mappings = build_issue_pr_mapping(issues)
    save_results(issues, mappings)


if __name__ == "__main__":
    main()
