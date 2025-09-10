# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import os
import io
import re

from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from scrapy.exceptions import DropItem
from google.auth.transport.requests import Request


class GoogleDrivePipeline:
    """
    Pipeline để upload scraped data lên Google Drive
    Tổ chức theo cấu trúc thư mục: root_folder/category/files
    """
    
    def __init__(self, oauth_key_file, oauth_token_file=None, parent_folder_id=None):
        self.oauth_key_file = oauth_key_file
        self.oauth_token_file = oauth_token_file
        self.parent_folder_id = parent_folder_id
        self.drive_service = None
        self.folder_cache = {}  # Cache để lưu folder IDs
        self.upload_stats = {
            'total_items': 0,
            'successful_uploads': 0,
            'failed_uploads': 0,
            'html_files': 0,
            'json_files': 0
        }
        
    @classmethod
    def from_crawler(cls, crawler):
        """Khởi tạo pipeline từ Scrapy settings"""
        return cls(
            oauth_key_file=crawler.settings.get('GOOGLE_OAUTH_KEY_FILE'),
            parent_folder_id=crawler.settings.get('GOOGLE_DRIVE_PARENT_FOLDER_ID'),
            oauth_token_file=crawler.settings.get('GOOGLE_OAUTH_TOKEN_FILE')
        )
    
    def open_spider(self, spider):
        """Khởi tạo khi spider bắt đầu"""
        try:
            # Kiểm tra file service account
            if not os.path.exists(self.oauth_key_file):
                raise Exception(f"Service account file not found: {self.oauth_key_file}")
            
            # Xác thực với Google Drive API
            # credentials = service_account.Credentials.from_service_account_file(
            #     self.service_account_file,
            #     scopes=['https://www.googleapis.com/auth/drive.file']
            # )

            credentials = None

            # Khởi tạo credentials bằng file token
            if self.oauth_token_file and  os.path.exists(self.oauth_token_file):
                credentials = Credentials.from_authorized_user_file(
                    self.oauth_token_file, 
                    scopes = ['https://www.googleapis.com/auth/drive.file']
                    )
            
            # Khởi tạo credential bằng file key (yêu cầu xác thực)
            if not credentials or not credentials.valid:
                if credentials and credentials.expired and credentials.refresh_token:
                    credentials.refresh(request=Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.oauth_key_file, ['https://www.googleapis.com/auth/drive.file']
                        ) 

                    credentials = flow.run_local_server(port=0)

                with open("token.json", "w") as token:
                    token.write(credentials.to_json())      
            
            self.drive_service = build('drive', 'v3', credentials=credentials)
            
            # Test connection
            about = self.drive_service.about().get(fields="user").execute()
            user = about.get('user', {})
            spider.logger.info(f"🔐 Authenticated as: {user.get('displayName', 'Unknown')} ({user.get('emailAddress', 'No email')})")
            
            # Tạo hoặc lấy root folder
            if not self.parent_folder_id:
                self.parent_folder_id = self._get_or_create_folder("scraped_hospital_data", None)
            
            spider.logger.info(f"✅ Google Drive Pipeline initialized")
            spider.logger.info(f"📁 Root folder ID: {self.parent_folder_id}")
            
        except Exception as e:
            spider.logger.error(f"❌ Failed to initialize Google Drive: {e}")
            raise DropItem(f"Failed to initialize Google Drive: {e}")
    
    def close_spider(self, spider):
        """Cleanup khi spider kết thúc"""
        spider.logger.info("🔒 Google Drive Pipeline closing...")
        
        # Log thống kê upload
        spider.logger.info("="*50)
        spider.logger.info("📊 UPLOAD STATISTICS")
        spider.logger.info("="*50)
        spider.logger.info(f"📄 Total items processed: {self.upload_stats['total_items']}")
        spider.logger.info(f"✅ Successful uploads: {self.upload_stats['successful_uploads']}")
        spider.logger.info(f"❌ Failed uploads: {self.upload_stats['failed_uploads']}")
        spider.logger.info(f"🌐 HTML files uploaded: {self.upload_stats['html_files']}")
        spider.logger.info(f"📝 TXT files uploaded: {self.upload_stats['txt_files']}")
        spider.logger.info(f"📁 Categories created: {len(self.folder_cache)}")
        spider.logger.info(f"🏷️ Category folders: {', '.join([k.split('_', 1)[-1] for k in self.folder_cache.keys() if '_' in k])}")
        spider.logger.info("="*50)
    

    """
    {
                'url': url,
                'page_content': response.body,
                'informations': informations,
                "crawled_at": strftime("%Y-%m-%d %H:%M:%S", gmtime()),
                "status": f"error: {str(e)}"
            }
    """
    def process_item(self, item, spider):
        """Xử lý từng item được scrapy trả về"""
        self.upload_stats['total_items'] += 1

        try:
            url = item.get('url')
            page_content = item.get('page_content')
            informations = item.get('informations', {})

            if not url:
                raise DropItem("Missing required field: url")

            if not page_content and not informations:
                raise DropItem("No content to upload: both page_content and texts are empty")

            # Detect category và slug từ URL
            category = self._detect_category(url)
            slug = self._detect_slug(url)

            uploaded_files = {}

            # Upload HTML file nếu có page_content
            if page_content:
                html_filename = f"{slug}.html"
                html_file_id = self._upload_file(
                    content=page_content,
                    filename=html_filename,
                    category=category,
                    url=url,
                    mimetype='text/html'
                )
                uploaded_files['html_file_id'] = html_file_id
                self.upload_stats['html_files'] += 1

                spider.logger.debug(f"📤 HTML uploaded: {category}/{html_filename} -> {html_file_id}")

            # Upload extracted texts dưới dạng .txt nếu có
            if informations:
                # Chọn folder cho text files: benh_text, thuoc_text, ...
                text_category = f"{category}_text"

                txt_content = informations.get('full_info', "")
                txt_filename = f"{slug}_texts.txt"

                txt_file_id = self._upload_file(
                    content=txt_content,
                    filename=txt_filename,
                    category=text_category,
                    url=url,
                    mimetype="text/plain"
                )
                uploaded_files['txt_file_id'] = txt_file_id
                self.upload_stats['txt_files'] = self.upload_stats.get('txt_files', 0) + 1

                spider.logger.debug(f"📤 TXT uploaded: {text_category}/{txt_filename} -> {txt_file_id}")

            # Log thành công
            files_info = []
            if uploaded_files.get('html_file_id'):
                files_info.append("HTML")
            if uploaded_files.get('txt_file_id'):
                files_info.append(f"TXT({len(txt_content)} charactes)")

            spider.logger.info(f"✅ {category}/{slug}: {' + '.join(files_info)}")

            # Thêm thông tin file IDs vào item
            item['uploaded_files'] = {
                **uploaded_files,
                'category': category,
                'slug': slug
            }

            self.upload_stats['successful_uploads'] += 1
            return item

        except Exception as e:
            self.upload_stats['failed_uploads'] += 1
            spider.logger.error(f"❌ Failed to upload {item.get('url', 'unknown')}: {e}")
            item['upload_error'] = str(e)
            return item

    
    def _detect_category(self, url):
        """Lấy category từ URL"""
        # Ví dụ: https://tamanhhospital.vn/benh/abc -> category = "benh"
        match = re.search(r"tamanhhospital\.vn/([^/]+)/", url)
        if match:
            category = match.group(1)
            # Loại bỏ các ký tự đặc biệt trong tên folder
            category = re.sub(r"[^a-zA-Z0-9_-]", "_", category)
            return category
        return "unknown"
    
    def _detect_slug(self, url):
        """Lấy slug từ URL"""
        # Lấy phần cuối cùng của URL làm slug
        slug = url.rstrip("/").split("/")[-1]
        # Loại bỏ các ký tự không hợp lệ cho tên file
        slug = re.sub(r"[^a-zA-Z0-9_.-]", "_", slug)
        
        # Xử lý trường hợp slug rỗng hoặc chỉ có extension
        if not slug or slug.startswith('.'):
            slug = "index"
        
        # Giới hạn độ dài filename (Google Drive limit ~255 chars)
        if len(slug) > 200:
            slug = slug[:200]
            
        return slug
    
    def _get_or_create_folder(self, folder_name, parent_id):
        """Tạo hoặc lấy folder ID, có cache để tránh tạo trùng"""
        cache_key = f"{parent_id}_{folder_name}"
        
        if cache_key in self.folder_cache:
            return self.folder_cache[cache_key]
        
        try:
            # Tìm folder đã tồn tại
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_id:
                query += f" and '{parent_id}' in parents"
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name)"
            ).execute()
            
            folders = results.get('files', [])
            
            if folders:
                # Folder đã tồn tại
                folder_id = folders[0]['id']
                self.folder_cache[cache_key] = folder_id
                return folder_id
            
            # Tạo folder mới
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_id:
                file_metadata['parents'] = [parent_id]
            
            folder = self.drive_service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            
            folder_id = folder.get('id')
            self.folder_cache[cache_key] = folder_id
            
            return folder_id
            
        except HttpError as error:
            raise Exception(f"Failed to create/get folder '{folder_name}': {error}")
    
    def _upload_file(self, content, filename, category, url, mimetype='text/html'):
        """Upload file lên Google Drive"""
        try:
            # Tạo hoặc lấy folder ID cho category
            category_folder_id = self._get_or_create_folder(category, self.parent_folder_id)
            
            # Kiểm tra file đã tồn tại chưa (optional - để overwrite hoặc skip)
            existing_file_id = self._check_file_exists(filename, category_folder_id)
            if existing_file_id:
                # Option 1: Skip file đã tồn tại
                # return existing_file_id
                
                # Option 2: Update file đã tồn tại (uncomment để sử dụng)
                return self._update_existing_file(existing_file_id, content, mimetype, url)
            
            # Tạo file metadata
            file_metadata = {
                'name': filename,
                'parents': [category_folder_id],
                'description': f'Scraped from: {url}\nCategory: {category}\nUploaded by: Hospital Crawler'
            }
            
            # Prepare content cho upload
            if isinstance(content, str):
                content = content.encode('utf-8')
            elif isinstance(content, bytes):
                pass  # content đã là bytes
            else:
                content = str(content).encode('utf-8')
            
            # Tạo media upload từ content
            media = MediaIoBaseUpload(
                io.BytesIO(content),
                mimetype=mimetype,
                resumable=True
            )
            
            # Upload file
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,webViewLink'
            ).execute()
            
            return file.get('id')
            
        except HttpError as error:
            raise Exception(f"Failed to upload file '{filename}': {error}")

    def _check_file_exists(self, filename, parent_folder_id):
        """Kiểm tra file đã tồn tại chưa"""
        try:
            # Escape single quotes trong filename để tránh lỗi query
            escaped_filename = filename.replace("'", "\\'")
            query = f"name='{escaped_filename}' and '{parent_folder_id}' in parents and trashed=false"
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name, createdTime)"
            ).execute()
            
            files = results.get('files', [])
            return files[0]['id'] if files else None
            
        except HttpError:
            return None

    def _update_existing_file(self, file_id, content, mimetype, url):
        """Update file đã tồn tại thay vì tạo mới"""
        try:
            # Prepare content
            if isinstance(content, str):
                content = content.encode('utf-8')
            elif isinstance(content, bytes):
                pass
            else:
                content = str(content).encode('utf-8')
            
            # Update metadata
            file_metadata = {
                'description': f'Scraped from: {url}\nLast updated by: Hospital Crawler'
            }
            
            # Tạo media upload
            media = MediaIoBaseUpload(
                io.BytesIO(content),
                mimetype=mimetype,
                resumable=True
            )
            
            # Update file
            updated_file = self.drive_service.files().update(
                fileId=file_id,
                body=file_metadata,
                media_body=media,
                fields='id,name,modifiedTime'
            ).execute()
            
            return updated_file.get('id')
            
        except HttpError as error:
            raise Exception(f"Failed to update existing file: {error}")
