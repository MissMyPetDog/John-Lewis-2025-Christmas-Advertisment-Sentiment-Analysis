import pandas as pd
import time
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# 假设 youtube 服务对象在外部已经建立，或者建议作为参数传入
# youtube = build('youtube', 'v3', developerKey=API_KEY)

def get_channel_title(youtube, video_id: str) -> str:
    """
    根据 video_id 获取发布该视频的博主名字（channelTitle）
    """
    try:
        request = youtube.videos().list(
            part="snippet",
            id=video_id
        )
        response = request.execute()

        items = response.get("items", [])
        if not items:
            return "UnknownChannel"

        return items[0]["snippet"].get("channelTitle", "UnknownChannel")

    except HttpError as e:
        print(f"[Channel Title Error] video={video_id}: {e}")
        return "UnknownChannel"


def sanitize_filename(s: str) -> str:
    """
    清理字符串，使其适合做文件名
    """
    if not s: return "untitled"
    # 替换非法字符
    s = re.sub(r'[\\/:*?"<>|]+', "_", s)
    # 替换换行符等
    s = s.replace('\n', ' ').replace('\r', '')
    s = s.strip()
    return s[:50] # 截断一下防止太长

def scrape_all_comments_for_single_video(
    youtube, # <--- 建议把 youtube 对象传进来
    video_id: str,
    top_max_pages: int = 50,
    reply_max_pages: int = 20,
    sleep_sec: float = 0.2 # 稍微增加一点延时，防止触发限流
) -> pd.DataFrame:
    
    rows = []
    
    # ---------- Step 1: 顶层评论循环 ----------
    page_token = None
    top_pages = 0

    print(f"🚀 Scrapping Video: {video_id}")

    while True:
        try:
            # 请求主楼
            request = youtube.commentThreads().list(
                part="snippet", 
                videoId=video_id,
                maxResults=100,
                pageToken=page_token,
                textFormat="plainText"
            )
            response = request.execute()

        except HttpError as e:
            print(f"[Top Error] video={video_id}: {e}")
            break

        items = response.get("items", [])
        if not items:
            break # 这一页没东西了，直接退

        # --- 遍历这一页的每一个主楼 ---
        for item in items:
            top_comment_id = item["snippet"]["topLevelComment"]["id"]
            top_snip = item["snippet"]["topLevelComment"]["snippet"]
            reply_count = item["snippet"].get("totalReplyCount", 0)

            # 1. 保存顶层评论
            rows.append({
                "video_id": video_id,
                "comment_id": top_comment_id,
                "parent_comment_id": None,
                "text": top_snip.get("textDisplay"),
                "author": top_snip.get("authorDisplayName"),
                "like_count": top_snip.get("likeCount", 0),
                "published_at": top_snip.get("publishedAt"),
                "comment_type": "top",
            })

            # 2. 如果有回复，去抓回复 (嵌套循环)
            if reply_count > 0:
                reply_token = None
                reply_pages = 0
                
                # print(f"   -> 发现 {reply_count} 条回复，正在展开...") # 调试用

                while True:
                    try:
                        reply_request = youtube.comments().list(
                            part="snippet",
                            parentId=top_comment_id,
                            maxResults=100,
                            pageToken=reply_token,
                            textFormat="plainText"
                        )
                        reply_resp = reply_request.execute()
                    except HttpError as e:
                        print(f"[Reply Error] parent={top_comment_id}: {e}")
                        break

                    reply_items = reply_resp.get("items", [])
                    for r in reply_items:
                        r_snip = r["snippet"]
                        rows.append({
                            "video_id": video_id,
                            "comment_id": r["id"],
                            "parent_comment_id": top_comment_id,
                            "text": r_snip.get("textDisplay"),
                            "author": r_snip.get("authorDisplayName"),
                            "like_count": r_snip.get("likeCount", 0),
                            "published_at": r_snip.get("publishedAt"),
                            "comment_type": "reply",
                        })

                    # 回复翻页逻辑
                    reply_token = reply_resp.get("nextPageToken")
                    reply_pages += 1
                    
                    if not reply_token or (reply_max_pages and reply_pages >= reply_max_pages):
                        break
                    
                    time.sleep(sleep_sec) # 休息一下

        # --- ⚠️ 关键修正：主楼翻页逻辑必须在 for 循环外面 ---
        page_token = response.get("nextPageToken")
        top_pages += 1
        
        # 打印进度
        if top_pages % 5 == 0:
            print(f"   ...Scrapped {len(rows)} Comments (Page {top_pages})")

        if not page_token:
            break

        if top_max_pages and top_pages >= top_max_pages:
            print(f"Approached Max Pages {top_max_pages}, Scrapping Aborting。")
            break
            
        time.sleep(sleep_sec)

    # 转换 DataFrame
    df = pd.DataFrame(rows)
    if not df.empty:
        # 去重
        df = df.drop_duplicates(subset=["video_id", "comment_id"]).reset_index(drop=True)
    
    print(f"✅ Scraped {len(df)} Comments.")
    return df