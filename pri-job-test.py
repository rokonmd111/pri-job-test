import os
import requests
import re
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import time
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta, date
from html.parser import HTMLParser
import json

# =========================================================
# ফোল্ডার এবং ফাইল পাথ সেটআপ
# =========================================================

def get_blogger_service() -> Optional[Any]:
    creds = None
    token_json = os.environ.get('BLOGGER_TOKEN_JSON') 
    client_secret_json = os.environ.get('CLIENT_SECRET_JSON')

    if token_json:
        info = json.loads(token_json)
        creds = Credentials.from_authorized_user_info(info, SCOPES)

# =========================================================
# কনফিগারেশন সেটিংস এবং API Endpoints
# =========================================================
API_BDS_LIST = os.environ.get('API_BDS_LIST')
API_BDS_DETAILS = os.environ.get('API_BDS_DETAILS')
APPLY_URL_BASE = os.environ.get('APPLY_URL_BASE')
BLOG_ID = os.environ.get('BLOG_ID')

SCOPES = ['https://www.googleapis.com/auth/blogger']
DELAY_AFTER_OPERATION = 10 

JOB_ID_LABEL_PREFIX = "BdJobID:"
END_DATE_LABEL_PREFIX = "BdEndDate:"
API_DATE_FORMATS = ['%Y-%m-%dT%H:%M:%SZ', '%m/%d/%Y %H:%M:%S']

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Host': 'gateway.bdjobs.com',
    'Referer': 'https://www.bdjobs.com/',
}

# =========================================================
# সহায়ক ফাংশন (Helper Functions)
# =========================================================

class HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)
    
def strip_html_tags(html_content: str) -> str:
    if not html_content:
        return ""
    try:
        stripper = HTMLStripper()
        stripper.feed(html_content)
        return stripper.get_data().strip().replace('\n', ' ').replace('\r', '').strip()
    except Exception:
        return html_content

def get_blogger_service() -> Optional[Any]:
    creds = None
    # গিটহাব সিক্রেট থেকে ডেটা সংগ্রহ
    token_json = os.environ.get('BLOGGER_TOKEN_JSON')
    client_secret_json = os.environ.get('CLIENT_SECRET_JSON')

    if token_json:
        try:
            info = json.loads(token_json)
            creds = Credentials.from_authorized_user_info(info, SCOPES)
        except Exception as e:
            print(f"❌ টোকেন লোড করতে ত্রুটি: {e}")
            return None

    # যদি টোকেন থাকে কিন্তু মেয়াদ শেষ হয়ে যায়, তবে রিফ্রেশ করবে
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as e:
            print(f"❌ টোকেন রিফ্রেশ করতে ব্যর্থ: {e}")
            return None

    # গিটহাবে রান করার সময় যদি উপরে creds তৈরি না হয়, তবে স্ক্রিপ্ট এখানেই থেমে যাবে
    if not creds:
        print("FATAL ERROR: কোনো বৈধ ক্রেডেনশিয়াল পাওয়া যায়নি। প্রথমে পিসিতে রান করে টোকেন নিন।")
        return None

    return build('blogger', 'v3', credentials=creds)

def format_api_date(date_str: str, format_list: List[str]) -> str:
    if not date_str:
        return "N/A"
    for fmt in format_list:
        try:
            dt_object = datetime.strptime(date_str.split('.')[0], fmt)
            dt_object_bdt = dt_object + timedelta(hours=6)
            return dt_object_bdt.strftime("%d-%m-%Y")
        except ValueError:
            continue
    return "N/A"

def parse_end_date_for_check(date_str: str) -> Optional[datetime.date]:
    if not date_str or date_str == "N/A":
        return None
    for fmt in ['%d-%m-%Y', '%Y-%m-%d']:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None

def check_for_contact_info(text: str) -> bool:
    """
    ⚠️⚠️ চূড়ান্ত সংশোধিত: কঠোরভাবে শুধুমাত্র Gmail বা বৈধ বাংলাদেশী ফোন নাম্বার যাচাই করা হবে। 
    অন্য ডোমেইনের ইমেইল পাওয়া গেলে বাতিল করা হবে।
    """
    if not text:
        return False
        
    # --- ফোন নাম্বার যাচাইকরণ ---
    
    # 1. টেক্সট থেকে শুধুমাত্র ডিজিটগুলো বের করে ফোন নাম্বার ফরম্যাট চেক করা।
    # ফোন নাম্বারে ব্যবহৃত হতে পারে এমন ক্যারেক্টার অপসারণ করা
    cleaned_phone_text = re.sub(r'[\s\-\(\)\.\+\/]', '', text) 
    
    # 2. বাংলাদেশের প্রচলিত মোবাইল নাম্বার ফরম্যাট (11 ডিজিট) যাচাই
    # এটি 01[3-9]XXXXXXXX ফরম্যাটের ১১ ডিজিটের নাম্বার খুঁজবে।
    phone_pattern_11_digit = r'\b(01[3-9]\d{8})\b'
    
    # 3. এটি +8801[3-9]XXXXXXXX ফরম্যাটের নাম্বার খুঁজবে (যদিও উপরেরটি কভার করবে, তবুও ব্যাকআপ)
    phone_pattern_13_digit = r'\b(\+8801[3-9]\d{8})\b' 

    if re.search(phone_pattern_11_digit, cleaned_phone_text) or re.search(phone_pattern_13_digit, text):
        print("       ✅ যোগাযোগ তথ্য পাওয়া গেছে: বৈধ বাংলাদেশি ফোন নাম্বার।")
        return True

    # --- ইমেইল যাচাইকরণ ---
    
    # 1. ইমেইল ফরম্যাট খোঁজা (যেকোনো ডোমেইন)
    # এই প্যাটার্নটি সমস্ত ইমেইল খুঁজে বের করবে
    email_pattern = r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
    all_emails = re.findall(email_pattern, text, re.IGNORECASE)
    
    if not all_emails:
        # যদি কোনো ফোন বা ইমেইল না পাওয়া যায়
        print("       ❌ কোনো বৈধ যোগাযোগের তথ্য (Gmail বা ফোন) পাওয়া যায়নি।")
        return False
        
    # 2. ইমেইল ডোমেইন যাচাইকরণ (Gmail চেক)
    valid_gmail_found = False
    
    for email in all_emails:
        if email.lower().endswith('@gmail.com'):
            valid_gmail_found = True
            break
            
        # ⚠️ কঠোরতা: যদি ইমেইল থাকে কিন্তু তা Gmail না হয়, তবে এই পোস্টটি বাতিল।
        # তবে এটি নিশ্চিত করার জন্য আগে ফোন নাম্বার চেক করা হয়েছে।
        # যদি Gmail পাওয়া যায়, তবেই বৈধতা পাবে।
    
    if valid_gmail_found:
        print("       ✅ যোগাযোগ তথ্য পাওয়া গেছে: বৈধ Gmail.")
        return True
        
    # যদি ফোন নাম্বার না পাওয়া যায় এবং Gmail-ও না পাওয়া যায়, তবে False রিটার্ন হবে।
    print("       ❌ কঠোরতা: শুধুমাত্র অন্য ডোমেইনের ইমেইল পাওয়া গেছে, যা বৈধ নয়। বাতিল করা হলো।")
    return False

# =========================================================
# ধাপ ১: API থেকে তালিকা ফেচ করা
# (অপরিবর্তিত)
# =========================================================

def fetch_job_list_from_page(session: requests.Session, page_num: int) -> List[Dict[str, Any]]:
    api_url = API_BDS_LIST.format(page_num=page_num)
    try:
        response = session.get(api_url, headers=HEADERS, timeout=20)
        response.raise_for_status()
        json_response = response.json()
        return json_response.get('data', [])
    except Exception as e:
        if hasattr(response, 'status_code') and response.status_code in [404, 400]:
            print(f"       - Page {page_num}: শেষ পেজে পৌঁছেছে বা Invalid Page।")
        else:
            print(f"       ❌ API লিস্ট ফেচ ব্যর্থ (Page {page_num}): {e}")
        return []

def fetch_all_target_jobs() -> Dict[str, Dict[str, Any]]:
    print("\n▶️ ধাপ ২: Bdjobs API থেকে সমস্ত তালিকা সংগ্রহ শুরু...")
    all_jobs: Dict[str, Dict[str, Any]] = {}
    session = requests.Session()
    time.sleep(2)
    
    current_date = date.today()
    print(f"   ⚠️ শুধুমাত্র {current_date.strftime('%d-%m-%Y')} বা তার পরের ডেডলাইন যুক্ত পোস্টগুলি সংগ্রহ করা হবে।")

    current_page = 1
    
    # ⚠️ শুধুমাত্র প্রথম পেজ রান করতে চাইলে নিচে দেওয়া 'MAX_PAGES_TO_FETCH' ভ্যারিয়েবলটি কমেন্ট আউট করে দিন
    MAX_PAGES_TO_FETCH = 4
    
    while True:
        if current_page > 1 and MAX_PAGES_TO_FETCH == 1:
            print(f"   ⚠️ শুধুমাত্র একটি পেজের উপর চলছে। সমস্ত পেজের জন্য 'MAX_PAGES_TO_FETCH' কমেন্ট আউট করুন।")
            break
        
        if current_page > MAX_PAGES_TO_FETCH: 
            break
        
        print(f"   🔎 Page {current_page} প্রক্রিয়াকরণ করা হচ্ছে...")
        job_list = fetch_job_list_from_page(session, current_page)
        
        if not job_list:
            break
        
        for job_item in job_list:
            job_id = str(job_item.get('Jobid'))
            title = job_item.get('jobTitle') or job_item.get('JobTitleBng', 'পদবিহীন').strip()
            company = job_item.get('companyName', 'অজানা সংস্থা').strip()
            deadline_db = job_item.get('deadlineDB')
            
            end_date_clean = format_api_date(deadline_db, API_DATE_FORMATS) if deadline_db else "N/A"
            
            job_end_date = parse_end_date_for_check(end_date_clean)
            
            if not job_end_date:
                print(f"       - ডেডলাইন অনুপস্থিত/ত্রুটিপূর্ণ (ID: {job_id})। এড়িয়ে যাওয়া হলো।")
                continue
            
            if job_end_date < current_date:
                print(f"       - মেয়াদ উত্তীর্ণ ({job_end_date.strftime('%d-%m-%Y')} < {current_date.strftime('%d-%m-%Y')}) (ID: {job_id})। এড়িয়ে যাওয়া হলো।")
                continue
            
            if job_id and len(title) > 2 and end_date_clean != "N/A":
                full_title = f"{title} - {company}"
                
                all_jobs[job_id] = {
                    'title': full_title,
                    'company_name': company,
                    'end_date_label': end_date_clean,
                    'page_order': current_page * 1000 + job_list.index(job_item)
                }
        
        current_page += 1
        time.sleep(1)

    print(f"✅ লক্ষ্য সাইট থেকে সংগ্রহ সম্পন্ন। মোট {len(all_jobs)} টি মেয়াদ শেষ না হওয়া পোস্ট পাওয়া গেছে।")
    return all_jobs


# =========================================================
# ধাপ ২: Job ID ব্যবহার করে বিস্তারিত ডেটা ফেচ করা
# (অপরিবর্তিত)
# =========================================================

def fetch_job_details_by_id(session: requests.Session, job_id: str) -> Optional[Dict[str, str]]:
    """Job ID ব্যবহার করে বিস্তারিত API কল করে সমস্ত ডেটা সংগ্রহ করে, এবং যোগাযোগের তথ্য যাচাই করে।"""
    print(f"       ⚙️ বিস্তারিত API কল শুরু (ID: {job_id})...")
    api_url = API_BDS_DETAILS.format(job_id=job_id)
    
    try:
        response = session.get(api_url, headers=HEADERS, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        details = data.get('data', [])[0] if data.get('data') else {}
        
        if not details:
            print("       ❌ বিস্তারিত JSON ডেটা পাওয়া যায়নি।")
            return None
        
        job_description_full = details.get('JobDescription', '')
        education_req_raw = details.get('EducationRequirements', '')
        experience_req_raw = details.get('experience', '')
        additional_req_raw = details.get('AdditionJobRequirements', '')
        read_before_apply_raw = details.get('RecruitmentProcessingInformation', '')
        apply_instruction_raw = details.get('ApplyInstruction', '')
        apply_email = details.get('ApplyEmail', '') 

        job_nature = details.get('JobNature', 'N/A')
        workplace = details.get('JobWorkPlace', 'N/A')
        job_location = details.get('JobLocation', 'N/A')
        salary_range = details.get('JobSalaryRange', 'Negotiable')
        apply_url = APPLY_URL_BASE.format(job_id=job_id)
        
        # --- ⚠️ কঠোর যোগাযোগ তথ্য যাচাইকরণ লজিক শুরু ⚠️ ---
        
        # সমস্ত সম্ভাব্য টেক্সট ফিল্ড একত্রিত করা
        all_text_content = job_description_full + " " + education_req_raw + " " + \
                           experience_req_raw + " " + additional_req_raw + " " + \
                           read_before_apply_raw + " " + apply_instruction_raw + " " + apply_email
                           
        # HTML ট্যাগ সরিয়ে প্লেইন টেক্সট যাচাই করা
        clean_text_for_check = strip_html_tags(all_text_content).strip()
        
        # যোগাযোগের তথ্য (Gmail বা Phone) যাচাই করা
        if not check_for_contact_info(clean_text_for_check):
            # যদি যোগাযোগের তথ্য বৈধ না হয় (Gmail বা Phone), তবে কঠোরভাবে বাতিল করা হলো।
            print(f"       ❌ কঠোরতা: এই পোস্ট বাতিল করা হলো (ID: {job_id})।")
            return None
        
        # --- যোগাযোগ তথ্য যাচাইকরণ লজিক শেষ ---
        
        print("       ✅ বিস্তারিত ডেটা সফলভাবে পাওয়া গেছে এবং যোগাযোগের তথ্য বৈধ।")
        return {
            'job_description_html': job_description_full,
            'apply_instruction_html': apply_instruction_raw,
            'read_before_apply_html': read_before_apply_raw,
            'education': education_req_raw,
            'experience': experience_req_raw,
            'additional_req': additional_req_raw,
            'job_nature': job_nature,
            'workplace': workplace,
            'job_location': job_location,
            'salary_range': salary_range,
            'apply_email': apply_email,
            'apply_url': apply_url
        }
            
    except Exception as e:
        print(f"       ❌ বিস্তারিত রিকোয়েস্ট/পার্সিং ব্যর্থ: {e}")
        return None

# =========================================================
# ধাপ ৩, ৪, ৫: ব্লগার ফেচিং, ডিলিট এবং অ্যাডিশন লজিক
# (অপরিবর্তিত)
# =========================================================

def fetch_blogger_posts(service: Any, blog_id: str) -> Dict[str, Dict[str, str]]:
    print("\n▶️ ধাপ ১: ব্লগার থেকে বর্তমান পোস্টের তালিকা সংগ্রহ শুরু...")
    published_jobs: Dict[str, Dict[str, str]] = {}
    
    try:
        response = service.posts().list(blogId=blog_id, fetchBodies=False, maxResults=500, labels='প্রাইভেট চাকরি').execute()
        posts = response.get('items', [])

        for post in posts:
            post_labels = post.get('labels', [])
            job_id = None
            end_date = None
            
            for label in post_labels:
                if label.startswith(JOB_ID_LABEL_PREFIX):
                    job_id = label[len(JOB_ID_LABEL_PREFIX):].strip()
                elif label.startswith(END_DATE_LABEL_PREFIX):
                    end_date = label[len(END_DATE_LABEL_PREFIX):].strip()
            
            if job_id:
                published_jobs[job_id] = {
                    'post_id': post['id'],
                    'title': post['title'],
                    'end_date': end_date
                }

    except Exception as e:
        print(f"❌ ব্লগার API থেকে ডেটা আনা ব্যর্থ হয়েছে: {e}")
    
    print(f"✅ ব্লগার থেকে সংগ্রহ সম্পন্ন। মোট {len(published_jobs)} টি {JOB_ID_LABEL_PREFIX[:-1]} যুক্ত পোস্ট পাওয়া গেছে।")
    return published_jobs

def perform_deletion(service: Any, blog_id: str, blogger_posts: Dict[str, Dict[str, str]]):
    print("\n▶️ ধাপ ৪: ডিলিট প্রক্রিয়া শুরু (মেয়াদ উত্তীর্ণ পোস্ট)...")
    
    ids_to_delete = []
    current_date = datetime.now().date()
    deletion_cutoff_date = current_date - timedelta(days=1)
    
    print(f"   🗑️ ডিলিট করার কাট-অফ ডেট: **{deletion_cutoff_date.strftime('%d-%m-%Y')}** (এই তারিখ বা এর আগে মেয়াদ শেষ হওয়া পোস্ট ডিলিট হবে)।")
    
    
    for job_id, post_data in blogger_posts.items():
        is_expired = False
        
        if post_data.get('end_date'):
            post_end_date = parse_end_date_for_check(post_data['end_date'])
            
            if post_end_date and post_end_date <= deletion_cutoff_date:
                is_expired = True

        if is_expired:
            ids_to_delete.append(post_data['post_id'])
            end_date_str = post_end_date.strftime('%d-%m-%Y') if post_end_date else 'N/A'
            print(f"       - ডিলিটের জন্য চিহ্নিত: ID {job_id} (End Date: {end_date_str})")

    if ids_to_delete:
        print(f"   🗑️ মোট **{len(ids_to_delete)}** টি মেয়াদ উত্তীর্ণ Bdjobs পোস্ট ডিলিট করা হবে।")
        for post_id in ids_to_delete:
            try:
                service.posts().delete(blogId=blog_id, postId=post_id).execute()
                print(f"       - পোস্ট ID {post_id} ডিলিট সম্পন্ন।")
                time.sleep(DELAY_AFTER_OPERATION)
            except Exception as e:
                print(f"       ❌ ডিলিট ব্যর্থ হয়েছে: পোস্ট ID {post_id}. ত্রুটি: {e}")
    else:
        print("   ✅ Bdjobs এর কোনো মেয়াদ উত্তীর্ণ পোস্ট ডিলিট করার মতো পাওয়া যায়নি।")
        
def perform_addition(service: Any, blog_id: str, target_posts: Dict[str, Dict[str, str]], blogger_posts: Dict[str, Dict[str, str]]):
    print("\n▶️ ধাপ ৫: নতুন পোস্ট প্রকাশের লজিক শুরু...")
    
    titles_to_add = {id: data for id, data in target_posts.items() if id not in blogger_posts}

    session = requests.Session()

    if titles_to_add:
        print(f"\n   ✍️ মোট **{len(titles_to_add)}** টি নতুন পোস্ট প্রকাশ করা শুরু হচ্ছে...")
        
        posts_to_add_sorted = sorted(titles_to_add.items(), key=lambda item: item[1]['page_order'])
        
        last_post_was_successful = False
        
        for job_id, data in posts_to_add_sorted:
            
            if last_post_was_successful:
                print(f"       ⏸️ পরবর্তী পোস্টের জন্য {DELAY_AFTER_OPERATION} সেকেন্ড অপেক্ষা করা হচ্ছে...")
                time.sleep(DELAY_AFTER_OPERATION)
            
            last_post_was_successful = False
            
            details_data = fetch_job_details_by_id(session, job_id)
            
            if not details_data:
                print(f"       ❌ এই পোস্টটিতে পর্যাপ্ত যোগাযোগের তথ্য না থাকায় এড়িয়ে যাওয়া হলো: {data['title']}.")
                continue
            
            final_end_date_label = data['end_date_label']
            
            # কন্টেন্ট তৈরি
            post_content = f"""
            <div style="padding: 15px; border: 1px solid #CC0000; background-color: #ffe0e0;">
                <h3 style="color: #CC0000; margin-top: 0;">আবেদনের শেষ তারিখ</h3>
                <p style="font-weight: bold; color: #CC0000;">{final_end_date_label} (সকাল ০৬:০০ টা পর্যন্ত)</p>
            </div>
            <hr/>
            <h3 style="color: #007456;">চাকরির সংক্ষিপ্ত তথ্য</h3>
            <p><strong>কাজের স্থান (Workplace):</strong> {details_data['workplace']}</p>
            <p><strong>কর্মসংস্থান অবস্থা (Employment Status):</strong> {details_data['job_nature']}</p>
            <p><strong>বেতন সীমা (Salary):</strong> {details_data['salary_range']}</p>
            <p><strong>চাকরির অবস্থান (Job Location):</strong> {details_data['job_location']}</p>
            <hr/>
            <h3 style="color: #007456;">দায়িত্ব ও প্রেক্ষাপট (Job Context and Responsibilities)</h3>
            {details_data['job_description_html']}
            <hr/>
            <h3 style="color: #007456;">যোগ্যতা ও অভিজ্ঞতা</h3>
            <p><strong>শিক্ষাগত যোগ্যতা (Education):</strong></p>
            {details_data['education']}
            <p><strong>অভিজ্ঞতা (Experience):</strong></p>
            {details_data['experience']}
            <p><strong>অতিরিক্ত প্রয়োজন (Additional Requirements):</strong></p>
            {details_data['additional_req']}
            <hr/>
            <h3 style="color: #007456;">আবেদনের প্রক্রিয়া ও যোগাযোগ</h3>
            <p style="font-weight: bold; color: #CC0000;">আবেদন করার আগে পড়ুন:</p>
            {details_data['read_before_apply_html']}
            
            <p style="font-weight: bold;">সম্পূর্ণ প্রক্রিয়া:</p>
            {details_data['apply_instruction_html']}
            <hr/>
            <p style="font-weight: bold;">সরাসরি আবেদনের লিঙ্ক: <a href="{details_data['apply_url']}" target="_blank">Bdjobs-এ আবেদন/বিস্তারিত দেখতে ক্লিক করুন</a></p>
            <p style="font-weight: bold;">যোগাযোগের ইমেইল (যদি থাকে): {details_data['apply_email']}</p>
            """
            
            # লেবেল তৈরি
            post_labels = ['জব সার্কুলার', 'প্রাইভেট চাকরি', data['company_name']]
            post_labels.append(f"{JOB_ID_LABEL_PREFIX}{job_id}")
            post_labels.append(f"{END_DATE_LABEL_PREFIX}{final_end_date_label}")

            post_body = {
                'kind': 'blogger#post',
                'title': data['title'],
                'content': post_content,
                'labels': post_labels,
                'isDraft': False
            }
            
            # পোস্ট করা
            try:
                service.posts().insert(blogId=blog_id, body=post_body).execute()
                print(f"       ✅ সফলভাবে প্রকাশিত: {data['title']}")
                last_post_was_successful = True
            except Exception as e:
                print(f"       ❌ API ERROR: পোস্ট করার সময় ব্যর্থ: {data['title']}. ত্রুটি: {e}")
                
    else:
        print("   ✅ কোনো নতুন পোস্ট প্রকাশের জন্য পাওয়া যায়নি।")
        
    print("\n✅ নতুন পোস্ট প্রকাশ প্রক্রিয়া সম্পন্ন হয়েছে।")


# =========================================================
# প্রধান নির্বাহ (Main Execution)
# =========================================================

def run_synchronization():
    """সিঙ্ক্রোনাইজেশন প্রক্রিয়া শুরু করে (আগে ডিলিট, পরে অ্যাডিশন)।"""
    print("--- Bdjobs Private Job Sync স্ক্রিপ্ট শুরু ---")
    
    blogger_service = get_blogger_service()
    if not blogger_service:
        print("❌ ব্লগার অথেন্টিকেশন ব্যর্থ। স্ক্রিপ্ট বাতিল করা হলো।")
        return
    
    blogger_posts = fetch_blogger_posts(blogger_service, BLOG_ID)
    perform_deletion(blogger_service, BLOG_ID, blogger_posts)
    target_posts = fetch_all_target_jobs()
    
    if not target_posts:
        print("❌ টার্গেট সাইট থেকে কোনো বৈধ পোস্ট ডেটা পাওয়া যায়নি। সিঙ্ক্রোনাইজেশন বাতিল করা হলো।")
        print("\n--- Bdjobs Private Job Sync স্ক্রিপ্ট সমাপ্ত ---")
        return
        
    perform_addition(blogger_service, BLOG_ID, target_posts, blogger_posts)
    
    print("\n--- Bdjobs Private Job Sync স্ক্রিপ্ট সমাপ্ত ---")


if __name__ == '__main__':
    run_synchronization()