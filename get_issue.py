import requests
import time
import json
import pandas as pd

BASE_URL = "https://issues.apache.org/jira/rest/api/2/search"

# ====== 你可以改这里 ======
JQL = """
project = KAFKA
AND issuetype in (Bug, Improvement)
ORDER BY created ASC
"""

PAGE_SIZE = 100       # 每页数量（<=1000）
SLEEP_SECONDS = 0.3   # 防止请求过快
# =========================


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
                "description",
                "status",
                "priority",
                "assignee",
                "reporter",
                "created",
                "updated",
                "resolutiondate"
            ])
        }

        print(f"Fetching issues: startAt={start_at}")
        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        issues = data.get("issues", [])

        if not issues:
            break

        all_issues.extend(issues)
        start_at += PAGE_SIZE

        time.sleep(SLEEP_SECONDS)

    return all_issues


def normalize_issue(issue):
    fields = issue["fields"]

    def safe_get(obj, path):
        for p in path:
            if obj is None:
                return None
            obj = obj.get(p)
        return obj

    return {
        "key": issue["key"],
        "summary": fields.get("summary"),
        "status": safe_get(fields, ["status", "name"]),
        "priority": safe_get(fields, ["priority", "name"]),
        "assignee": safe_get(fields, ["assignee", "displayName"]),
        "reporter": safe_get(fields, ["reporter", "displayName"]),
        "created": fields.get("created"),
        "updated": fields.get("updated"),
        "resolutiondate": fields.get("resolutiondate"),
        "description": fields.get("description"),
    }


def save_results(issues):
    # 原始 JSON
    with open("kafka_jira_raw.json", "w", encoding="utf-8") as f:
        json.dump(issues, f, ensure_ascii=False, indent=2)

    # 扁平化
    records = [normalize_issue(issue) for issue in issues]
    df = pd.DataFrame(records)

    df.to_csv("kafka_jira_issues.csv", index=False)
    df.to_json("kafka_jira_issues.json", orient="records", force_ascii=False)

    print(f"Saved {len(df)} issues")


def main():
    issues = fetch_all_issues()
    save_results(issues)


if __name__ == "__main__":
    main()
