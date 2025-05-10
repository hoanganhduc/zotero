#!/usr/bin/env python3
# Standard library imports
import argparse
import concurrent.futures
import json
import os
import html
import io
import sys
import re
import sqlite3
from datetime import datetime

# Third-party imports
import pdfkit
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pathlib
from googleapiclient.http import MediaIoBaseDownload
import tempfile
import platform

# Google Drive API imports
def authenticate_google_drive(service_account_file):
    SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
    creds = None
    service_info = {}
    try:
        if service_account_file.strip().startswith('{') and service_account_file.strip().endswith('}'):
            service_info = json.loads(service_account_file)
            service_json_io = io.StringIO(service_account_file)
            creds = service_account.Credentials.from_service_account_info(
                service_info, scopes=SCOPES)
            print("Authenticated using provided JSON string")
        else:
            if not os.path.exists(service_account_file):
                print(f"Error: Service account key file not found: {service_account_file}")
                return None
            with open(service_account_file, 'r') as f:
                service_info = json.load(f)
            creds = service_account.Credentials.from_service_account_file(
                service_account_file, scopes=SCOPES)
            print(f"Authenticated using service account file: {service_account_file}")
        service_email = service_info.get('client_email', 'unknown-service-account')
        print(f"Authenticated as service account: {service_email}")
    except Exception as e:
        print(f"Error authenticating with service account: {str(e)}")
        return None
    return creds

def test_google_drive_access(google_creds, verbose=False):
    if verbose:
        print("Testing Google Drive access using provided credentials")
    try:
        if not google_creds:
            return False, "No credentials provided. Authentication failed."
        drive_service = build('drive', 'v3', credentials=google_creds)
        about = drive_service.about().get(fields="user,storageQuota").execute()
        files = drive_service.files().list(
            pageSize=1, 
            fields="files(id,name),nextPageToken"
        ).execute()
        service_email = 'Unknown'
        if hasattr(google_creds, 'service_account_email'):
            service_email = google_creds.service_account_email
        elif 'user' in about and 'emailAddress' in about['user']:
            service_email = about['user']['emailAddress']
        storage_used = int(about.get('storageQuota', {}).get('usage', 0)) / (1024 * 1024)
        storage_total = int(about.get('storageQuota', {}).get('limit', 0)) / (1024 * 1024 * 1024)
        file_count = "at least 1" if files.get('files') else "0"
        if 'nextPageToken' in files:
            file_count = "more than 100"
        message = (
            f"Successfully connected to Google Drive!\n"
            f"Service Account: {service_email}\n"
            f"Storage used: {storage_used:.2f} MB / {storage_total:.2f} GB\n"
            f"Files: {file_count}"
        )
        return True, message
    except Exception as e:
        error_message = f"Error accessing Google Drive: {str(e)}"
        if verbose:
            print(error_message)
        return False, error_message

def search_file_in_drive(drive_service, query, max_results=10, folder_name=None, include_shared=True):
    results = []
    page_token = None
    folder_id = None
    if folder_name:
        folder_query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        folder_response = drive_service.files().list(
            q=folder_query,
            spaces='drive',
            fields='files(id, name)',
            pageSize=1).execute()
        folders = folder_response.get('files', [])
        if folders:
            folder_id = folders[0]['id']
            query = f"{query} and '{folder_id}' in parents"
    while True:
        search_params = {
            'q': query,
            'spaces': 'drive',
            'fields': 'nextPageToken, files(id, name, webViewLink)',
            'pageToken': page_token,
            'pageSize': max_results
        }
        response = drive_service.files().list(**search_params).execute()
        results.extend(response.get('files', []))
        if include_shared and len(results) < max_results and not folder_id:
            shared_query = f"{query} and sharedWithMe=true"
            shared_response = drive_service.files().list(
                q=shared_query,
                spaces='drive',
                fields='files(id, name, webViewLink)',
                pageSize=max_results - len(results)
            ).execute()
            shared_files = shared_response.get('files', [])
            existing_ids = {file['id'] for file in results}
            for file in shared_files:
                if file['id'] not in existing_ids:
                    results.append(file)
                    existing_ids.add(file['id'])
        page_token = response.get('nextPageToken', None)
        if page_token is None or len(results) >= max_results:
            break
    return results[:max_results]

def get_drive_url_by_filename(google_creds, filename, exact_match=True, folder_name=None, return_all=False, verbose=False):
    try:
        if verbose:
            print(f"Searching for file: {filename} in Google Drive")
        if not google_creds:
            if verbose:
                print("No valid Google credentials provided")
            return None
        drive_service = build('drive', 'v3', credentials=google_creds)
        safe_filename = filename.replace("'", "\\'")
        if exact_match:
            query = f"name = '{safe_filename}' and trashed = false"
        else:
            query = f"name contains '{safe_filename}' and trashed = false"
        results = search_file_in_drive(drive_service, query, max_results=10 if return_all else 1, folder_name=folder_name)
        if verbose:
            print(f"Found {len(results)} matching files")
        if not results:
            return None
        elif return_all:
            return [item.get('webViewLink') for item in results if 'webViewLink' in item]
        else:
            return results[0].get('webViewLink')
    except Exception as e:
        print(f"Error accessing Google Drive: {str(e)}", file=sys.stderr)
        return None

def print_progress(message, verbose=True, level=1, file=sys.stdout):
    if verbose:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}", file=file)
        file.flush()

# Calibre functions
def connect_to_calibre_db(library_path, google_creds=None, verbose=False):
    db_path = os.path.join(library_path, 'metadata.db')
    if os.path.exists(db_path):
        return sqlite3.connect(db_path)
    # If not found locally, try to find in Google Drive
    if google_creds:
        print_progress(f"Local Calibre database not found at {db_path}. Searching in Google Drive...", verbose)
        filename = 'metadata.db'
        # Try to find any folder named "Calibre Library" in Google Drive
        try:
            drive_service = build('drive', 'v3', credentials=google_creds)
            # Search for folders named "Calibre Library"
            folder_query = "name = 'Calibre Library' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            folder_response = drive_service.files().list(
                q=folder_query,
                spaces='drive',
                fields='files(id, name)',
                pageSize=10
            ).execute()
            folders = folder_response.get('files', [])
            for folder in folders:
                folder_id = folder['id']
                # Search for metadata.db in this folder
                file_query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
                results = drive_service.files().list(
                    q=file_query,
                    spaces='drive',
                    fields='files(id, name)',
                    pageSize=1
                ).execute().get('files', [])
                if results:
                    file_id = results[0]['id']
                    temp_dir = tempfile.gettempdir()
                    temp_db_path = os.path.join(temp_dir, 'metadata.db')
                    request = drive_service.files().get_media(fileId=file_id)
                    with open(temp_db_path, 'wb') as f:
                        downloader = MediaIoBaseDownload(f, request)
                        done = False
                        while not done:
                            status, done = downloader.next_chunk()
                            if verbose:
                                print_progress(f"Download progress: {int(status.progress() * 100)}%", verbose)
                    print_progress(f"Downloaded metadata.db from Google Drive folder '{folder['name']}' to {temp_db_path}", verbose)
                    return sqlite3.connect(temp_db_path)
            # Fallback: try searching anywhere in Drive (legacy behavior)
            drive_url = get_drive_url_by_filename(google_creds, filename, exact_match=True, folder_name=None, verbose=verbose)
            if drive_url:
                results = search_file_in_drive(drive_service, f"name = '{filename}' and trashed = false", max_results=1)
                if results:
                    file_id = results[0]['id']
                    temp_dir = tempfile.gettempdir()
                    temp_db_path = os.path.join(temp_dir, 'metadata.db')
                    request = drive_service.files().get_media(fileId=file_id)
                    with open(temp_db_path, 'wb') as f:
                        downloader = MediaIoBaseDownload(f, request)
                        done = False
                        while not done:
                            status, done = downloader.next_chunk()
                            if verbose:
                                print_progress(f"Download progress: {int(status.progress() * 100)}%", verbose)
                    print_progress(f"Downloaded metadata.db from Google Drive to {temp_db_path}", verbose)
                    return sqlite3.connect(temp_db_path)
            print_progress("metadata.db not found in any 'Calibre Library' folder or anywhere in Google Drive.", verbose, file=sys.stderr)
        except Exception as e:
            print_progress(f"Failed to download metadata.db from Google Drive: {e}", verbose, file=sys.stderr)
            raise FileNotFoundError(f"Could not find or download Calibre database: {e}")
    raise FileNotFoundError(f"Calibre database not found at {db_path} and not found in Google Drive.")

def list_calibre_books(conn, categories=None):
    cursor = conn.cursor()
    base_query = """
        SELECT 
            books.id, books.title, books.path, books.pubdate, books.isbn, 
            books.series_index AS series_index,
            s.name as series,
            p.name as publisher,
            books.timestamp
        FROM books
        LEFT JOIN books_series_link bsl ON books.id = bsl.book
        LEFT JOIN series s ON bsl.series = s.id
        LEFT JOIN books_publishers_link bpl ON books.id = bpl.book
        LEFT JOIN publishers p ON bpl.publisher = p.id
    """
    params = []
    if categories:
        # Join with tags table to filter by categories (tags)
        base_query += """
            JOIN books_tags_link btl ON books.id = btl.book
            JOIN tags t ON btl.tag = t.id
            WHERE t.name IN ({})
        """.format(','.join(['?'] * len(categories)))
        params.extend(categories)
    # Order by added time (timestamp), most recent first
    base_query += " ORDER BY books.timestamp DESC"
    cursor.execute(base_query, params)
    books = cursor.fetchall()
    # Get authors for each book
    book_list = []
    for book in books:
        book_id, title, path, pubdate, isbn, series_index, series, publisher, timestamp = book
        cursor.execute("""
            SELECT a.name FROM authors a
            JOIN books_authors_link l ON a.id = l.author
            WHERE l.book = ?
            ORDER BY l.id
        """, (book_id,))
        authors = [row[0] for row in cursor.fetchall()]
        # Get formats
        cursor.execute("""
            SELECT format, name FROM data WHERE book = ?
        """, (book_id,))
        formats = [{'format': row[0], 'name': row[1]} for row in cursor.fetchall()]
        book_list.append({
            'id': book_id,
            'title': title,
            'authors': authors,
            'path': path,
            'pubdate': pubdate,
            'isbn': isbn,
            'series': series,
            'series_index': series_index,
            'publisher': publisher,
            'formats': formats,
            'timestamp': timestamp
        })
    return book_list

def get_attachment_paths(book, library_path, google_creds=None, verbose=False):
    attachment_info = []
    # Use pathlib for cross-platform path handling
    library_path = pathlib.Path(library_path)
    book_folder = library_path / book['path']
    for fmt in book['formats']:
        ext = fmt['format'].lower()
        # Ensure filename includes extension
        if '.' in fmt['name']:
            filename = fmt['name']
        else:
            filename = f"{fmt['name']}.{ext}"
        local_path = book_folder / filename
        # Convert to absolute path and then to POSIX (Linux-style) path
        local_path_str = str(local_path.resolve().as_posix())
        # Remove any prefix before "Calibre Library" in the local_path
        idx = local_path_str.lower().find("calibre library".lower())
        if idx != -1:
            local_path_str = local_path_str[idx:]
        # Always append the extension to the local_path if not present
        if not local_path_str.lower().endswith(f".{ext}"):
            local_path_str = f"{local_path_str}.{ext}"
        info = {'local_path': local_path_str, 'drive_url': None}
        if google_creds:
            if verbose:
                print_progress(f"Searching for {filename} in Google Drive", verbose)
            try:
                drive_url = get_drive_url_by_filename(
                    google_creds, 
                    filename, 
                    exact_match=True,
                    folder_name=None, 
                    verbose=verbose
                )
                if drive_url:
                    info['drive_url'] = drive_url
                    if verbose:
                        print_progress(f"Found Google Drive URL for {filename}", verbose)
            except Exception as e:
                if verbose:
                    print_progress(f"Error searching Google Drive for {filename}: {e}", verbose, file=sys.stderr)
        attachment_info.append(info)
    return attachment_info

def format_book_text(book, library_path, google_creds=None, verbose=False):
    output = []
    output.append(f"Title: {book['title']}")
    if book['authors']:
        output.append(f"Authors: {'; '.join(book['authors'])}")
    if book['series']:
        output.append(f"Series: {book['series']} ({book['series_index']})")
    if book['publisher']:
        output.append(f"Publisher: {book['publisher']}")
    if book['pubdate']:
        output.append(f"Date: {book['pubdate']}")
    if book['isbn']:
        output.append(f"ISBN: {book['isbn']}")
    attachments = get_attachment_paths(book, library_path, google_creds, verbose)
    if attachments:
        output.append("Attachments:")
        for attachment in attachments:
            local_path = attachment.get('local_path', 'Unknown')
            drive_url = attachment.get('drive_url')
            if drive_url:
                output.append(f"  - {local_path} (Drive: {drive_url})")
            else:
                output.append(f"  - {local_path}")
    return "\n".join(output)

def format_book_html(book, library_path, google_creds=None, verbose=False):
    html_parts = [f"<div class='item'>"
                 f"<h2>{html.escape(book['title'] or 'Unknown')}</h2>"]
    if book['authors']:
        html_parts.append(f"<p><strong>Authors:</strong> {html.escape('; '.join(book['authors']))}</p>")
    if book['series']:
        html_parts.append(f"<p><strong>Series:</strong> {html.escape(book['series'])} ({book['series_index']})</p>")
    if book['publisher']:
        html_parts.append(f"<p><strong>Publisher:</strong> {html.escape(book['publisher'])}</p>")
    if book['pubdate']:
        html_parts.append(f"<p><strong>Date:</strong> {html.escape(str(book['pubdate']))}</p>")
    if book['isbn']:
        html_parts.append(f"<p><strong>ISBN:</strong> {html.escape(book['isbn'])}</p>")
    attachments = get_attachment_paths(book, library_path, google_creds, verbose)
    if attachments:
        html_parts.append("<p><strong>Attachments:</strong></p>")
        html_parts.append("<ul>")
        for attachment in attachments:
            local_path = html.escape(attachment.get('local_path', 'Unknown'))
            drive_url = attachment.get('drive_url')
            if drive_url:
                html_parts.append(f"<li>{local_path} - <a href='{html.escape(drive_url)}' target='_blank'>View on Google Drive</a></li>")
            else:
                html_parts.append(f"<li>{local_path}</li>")
        html_parts.append("</ul>")
    html_parts.append("</div>")
    return "\n".join(html_parts)

def generate_text_output(books, library_path, google_creds=None, verbose=False):
    if verbose:
        print_progress("Starting text output generation", verbose)
    current_date = datetime.now().strftime("%Y-%m-%d")
    title = f"Calibre Library - {current_date}"
    header = [
        title,
        "=" * len(title),
        ""
    ]
    def format_single_book(idx, book):
        try:
            item_header = f"Book #{idx+1}"
            item_content = format_book_text(book, library_path, google_creds, verbose)
            return f"{item_header}\n{item_content}\n---"
        except Exception as e:
            error_msg = f"Error formatting book {idx+1}: {e}"
            print_progress(error_msg, verbose, file=sys.stderr)
            return f"{error_msg}\n---"
    formatted_books = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_idx = {}
        for i, book in enumerate(books):
            future = executor.submit(format_single_book, i, book)
            future_to_idx[future] = i
        completed = 0
        for future in concurrent.futures.as_completed(future_to_idx):
            completed += 1
            if verbose and (completed % 10 == 0 or completed == len(books)):
                print_progress(f"Completed {completed}/{len(books)} books", verbose)
            idx = future_to_idx[future]
            try:
                formatted_books.append((idx, future.result()))
            except Exception as e:
                error_msg = f"Error processing book {idx+1}: {e}"
                print_progress(error_msg, verbose, file=sys.stderr)
                formatted_books.append((idx, f"{error_msg}\n---"))
    formatted_books.sort(key=lambda x: x[0])
    ordered_books = [text for _, text in formatted_books]
    if verbose:
        print_progress("Text output generation complete", verbose)
    return "\n".join(header + ordered_books)

def generate_html_header(title, notice=None):
    default_notice = "This document was automatically generated from a Calibre library. Items are listed for personal reference only. All references, articles, and other content remain the property of their respective copyright holders. This document is not for redistribution. Last updated on " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "."
    notice_text = notice if notice is not None else default_notice
    return [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        f"<title>{title}</title>",
        "<style>",
        "body { font-family: Arial, sans-serif; margin: 40px; }",
        ".item { margin-bottom: 30px; border-bottom: 1px solid #ccc; padding-bottom: 20px; }",
        ".item-number { font-weight: bold; color: #7f8c8d; margin-bottom: 5px; }",
        "h1 { color: #2c3e50; }",
        "h2 { color: #3498db; }",
        ".notice { font-style: italic; background-color: #f8f9fa; padding: 10px; border-left: 3px solid #3498db; margin-bottom: 20px; }",
        ".coffee-button { position: absolute; top: 20px; right: 20px; }",
        ".coffee-button img { height: 40px; border: none; }",
        ".search-container { margin-bottom: 20px; padding: 15px; background-color: #f8f9fa; border-radius: 5px; }",
        "#searchInput { width: 300px; padding: 8px; font-size: 16px; border: 1px solid #ccc; border-radius: 4px; }",
        "#searchBtn { padding: 8px 15px; background-color: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer; margin-left: 10px; }",
        "#searchBtn:hover { background-color: #2980b9; }",
        "#searchCount { margin-left: 15px; font-style: italic; }",
        ".highlight { background-color: yellow; font-weight: bold; }",
        ".hidden { display: none; }",
        "</style>",
        "</head>",
        "<body>",
        "<div class='coffee-button'>",
        "<a href='https://www.buymeacoffee.com/hoanganhduc' target='_blank'>",
        "<img src='https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png' alt='Buy Me A Coffee'>",
        "</a>",
        "</div>",
        f"<h1>{title}</h1>",
        f"<div class='notice'>{notice_text}</div>"
    ]

def generate_search_container():
    return [
        "<div class='search-container'>",
        "<input type='text' id='searchInput' placeholder='Search within this page...' />",
        "<button id='searchBtn'>Search</button>",
        "<span id='searchCount'></span>",
        "</div>"
    ]

def format_single_book(idx, book, library_path, google_creds, verbose):
    try:
        item_number = f"<div class='item-number'>Book #{idx+1}</div>"
        item_content = format_book_html(book, library_path, google_creds, verbose)
        return item_number + "\n" + item_content
    except Exception as e:
        error_msg = f"Error formatting book {idx+1}: {e}"
        print_progress(error_msg, verbose, file=sys.stderr)
        return f"<div class='item-error'>{error_msg}</div>"

def generate_books_html(books, library_path, google_creds, verbose):
    if verbose:
        print_progress(f"Preparing to format {len(books)} books simultaneously", verbose)
    formatted_books = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_idx = {}
        for i, book in enumerate(books):
            future = executor.submit(format_single_book, i, book, library_path, google_creds, verbose)
            future_to_idx[future] = i
        completed = 0
        for future in concurrent.futures.as_completed(future_to_idx):
            completed += 1
            if verbose and (completed % 10 == 0 or completed == len(books)):
                print_progress(f"Completed {completed}/{len(books)} books", verbose)
            idx = future_to_idx[future]
            try:
                formatted_books.append((idx, future.result()))
            except Exception as e:
                error_msg = f"Error processing book {idx+1}: {e}"
                print_progress(error_msg, verbose, file=sys.stderr)
                formatted_books.append((idx, f"<div class='item-error'>{error_msg}</div>"))
    formatted_books.sort(key=lambda x: x[0])
    return [html_content for _, html_content in formatted_books]

def generate_search_script():
    return [
        "<script>",
        "document.addEventListener('DOMContentLoaded', function() {",
        "  const searchInput = document.getElementById('searchInput');",
        "  const searchBtn = document.getElementById('searchBtn');",
        "  const searchCount = document.getElementById('searchCount');",
        "  const items = document.querySelectorAll('.item');",
        "",
        "  function performSearch() {",
        "    const searchTerm = searchInput.value.toLowerCase().trim();",
        "    if (searchTerm === '') {",
        "      items.forEach(item => {",
        "        item.classList.remove('hidden');",
        "        const highlighted = item.querySelectorAll('.highlight');",
        "        highlighted.forEach(el => {",
        "          const parent = el.parentNode;",
        "          parent.replaceChild(document.createTextNode(el.textContent), el);",
        "          parent.normalize();",
        "        });",
        "      });",
        "      searchCount.textContent = '';",
        "      return;",
        "    }",
        "    let matchCount = 0;",
        "    items.forEach(item => {",
        "      const text = item.textContent.toLowerCase();",
        "      const hasMatch = text.includes(searchTerm);",
        "      if (hasMatch) {",
        "        item.classList.remove('hidden');",
        "        matchCount++;",
        "        highlightText(item, searchTerm);",
        "      } else {",
        "        item.classList.add('hidden');",
        "      }",
        "    });",
        "    searchCount.textContent = `Found ${matchCount} matching items`;",
        "  }",
        "  function highlightText(element, searchTerm) {",
        "    const highlighted = element.querySelectorAll('.highlight');",
        "    highlighted.forEach(el => {",
        "      const parent = el.parentNode;",
        "      parent.replaceChild(document.createTextNode(el.textContent), el);",
        "      parent.normalize();",
        "    });",
        "    function processNode(node) {",
        "      if (node.nodeType === 3) {",
        "        const text = node.nodeValue.toLowerCase();",
        "        const index = text.indexOf(searchTerm.toLowerCase());",
        "        if (index >= 0) {",
        "          const before = node.nodeValue.substring(0, index);",
        "          const match = node.nodeValue.substring(index, index + searchTerm.length);",
        "          const after = node.nodeValue.substring(index + searchTerm.length);",
        "          const beforeNode = document.createTextNode(before);",
        "          const matchNode = document.createElement('span');",
        "          matchNode.classList.add('highlight');",
        "          matchNode.textContent = match;",
        "          const afterNode = document.createTextNode(after);",
        "          const parent = node.parentNode;",
        "          parent.replaceChild(afterNode, node);",
        "          parent.insertBefore(matchNode, afterNode);",
        "          parent.insertBefore(beforeNode, matchNode);",
        "          processNode(afterNode);",
        "        }",
        "      } else if (node.nodeType === 1 && node.childNodes && !/(script|style)/i.test(node.tagName)) {",
        "        Array.from(node.childNodes).forEach(child => processNode(child));",
        "      }",
        "    }",
        "    processNode(element);",
        "  }",
        "  searchBtn.addEventListener('click', performSearch);",
        "  searchInput.addEventListener('keyup', function(event) {",
        "    if (event.key === 'Enter') {",
        "      performSearch();",
        "    }",
        "  });",
        "});",
        "</script>",
        "</body>",
        "</html>"
    ]

def generate_html_output(books, library_path, google_creds=None, verbose=False, notice=None):
    if verbose:
        print_progress("Starting HTML output generation", verbose)
    current_date = datetime.now().strftime("%Y-%m-%d")
    title = f"Calibre Library - {current_date}"
    html_parts = []
    html_parts.extend(generate_html_header(title, notice))
    html_parts.extend(generate_search_container())
    formatted_books = generate_books_html(books, library_path, google_creds, verbose)
    html_parts.extend(formatted_books)
    html_parts.extend(generate_search_script())
    if verbose:
        print_progress("HTML output generation complete", verbose)
    return "\n".join(html_parts)

def generate_pdf_output(html_content, output_file, verbose=False):
    if verbose:
        print_progress("Starting PDF generation...", verbose)
        html_size_kb = len(html_content) / 1024
        print_progress(f"Using pdfkit to process approximately {html_size_kb:.1f} KB of HTML content", verbose)
    try:
        options = {
            'quiet': not verbose,
            'encoding': "UTF-8",
        }
        pdfkit.from_string(html_content, output_file, options=options)
        if os.path.exists(output_file):
            pdf_size_kb = os.path.getsize(output_file) / 1024
            print_progress(f"PDF successfully generated ({pdf_size_kb:.1f} KB) and saved to {output_file}", verbose)
        else:
            print_progress("PDF generation seemed to complete but output file not found", verbose, file=sys.stderr)
    except Exception as e:
        print_progress(f"Error generating PDF with pdfkit: {str(e)}", verbose, file=sys.stderr)
        sys.exit(1)

def display_books(books, output_format, output_file=None, library_path=None, verbose=False, google_creds=None, notice=None):
    if not books:
        print("No books found.")
        return
    print_progress("Displaying books...", verbose)
    if output_format == 'text':
        print_progress("Generating text output...", verbose)
        text_content = generate_text_output(books, library_path, google_creds, verbose)
        if output_file:
            print_progress(f"Saving text output to {output_file}", verbose)
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(text_content)
            print(f"Text output saved to {output_file}")
        else:
            print_progress("Displaying text output to console", verbose)
            print(text_content)
    elif output_format == 'html':
        print_progress("Generating HTML output...", verbose)
        html_content = generate_html_output(books, library_path, google_creds, verbose, notice)
        if output_file:
            print_progress(f"Saving HTML output to {output_file}", verbose)
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"HTML output saved to {output_file}")
        else:
            print_progress("Displaying HTML output to console", verbose)
            print(html_content)
    elif output_format == 'pdf':
        print_progress("Generating PDF output...", verbose)
        html_content = generate_html_output(books, library_path, google_creds, verbose, notice)
        if not output_file:
            output_file = "calibre_books.pdf"
            print_progress(f"No output file specified, using default: {output_file}", verbose)
        generate_pdf_output(html_content, output_file, verbose)
        print(f"PDF output saved to {output_file}")
    print_progress("Book display complete", verbose)

def parse_arguments():
    system = platform.system()
    if system == "Windows":
        user_profile = os.environ.get('USERPROFILE', r'C:\Users\Default')
        default_library_path = os.path.join(user_profile, 'Calibre Library')
    elif system == "Darwin":
        default_library_path = os.path.expanduser('~/Calibre Library')
    else:  # Assume Linux/Unix
        default_library_path = os.path.expanduser('~/Calibre Library')

    parser = argparse.ArgumentParser(description='List items from a Calibre library.')
    parser.add_argument('-l', '--library-path', default=default_library_path,
                        help=f'Path to Calibre library folder (default: {default_library_path})')
    parser.add_argument('-o', '--output-format', choices=['text', 'html', 'pdf'], default='text',
                        help='Output format (default: text)')
    parser.add_argument('-f', '--output-file', help='Output file name (for html and pdf)')
    parser.add_argument('-v', '--verbose', action='store_true', 
                        help='Display progress information during execution')
    parser.add_argument('-s', '--service-account-file', 
                        help='Path to Google service account JSON file or the JSON string itself for Drive integration')
    parser.add_argument('-n', '--notice', 
                        help='Custom copyright notice message for HTML/PDF output (uses a default message if not specified)')
    parser.add_argument('-t', '--tag', action='append',
                        help='Specify a tag to filter books (can be used multiple times for multiple tags)')
    return parser.parse_args()

def main():
    try:
        args = parse_arguments()
        print_progress("Connecting to Calibre library...", args.verbose)
        google_creds = None
        if args.service_account_file:
            print_progress("Authenticating with Google Drive using service account...", args.verbose)
            google_creds = authenticate_google_drive(args.service_account_file)
            if google_creds:
                print_progress("Testing Google Drive access...", args.verbose)
                success, message = test_google_drive_access(google_creds, verbose=args.verbose)
                if success:
                    print_progress("Google Drive access verified successfully!", args.verbose)
                    if args.verbose:
                        print_progress(message, args.verbose)
                else:
                    print_progress("Google Drive access failed!", args.verbose, level=3, file=sys.stderr)
                    if args.verbose:
                        print_progress(message, args.verbose, file=sys.stderr)
                    print_progress("The script will continue, but Google Drive links won't be available.", args.verbose)
            else:
                print_progress("Google Drive authentication failed. Google Drive integration will be disabled.", args.verbose, file=sys.stderr)
        else:
            print_progress("No Google Drive service account file provided. Google Drive integration will be disabled.", args.verbose)
        conn = connect_to_calibre_db(args.library_path, google_creds=google_creds, verbose=args.verbose)
        print_progress("Connection established successfully", args.verbose)
        # Handle tags if specified
        categories = None
        if args.tag:
            # Lowercase all tags for case-insensitive matching
            categories = [cat.strip().lower() for cat in args.tag if cat.strip()]
            print_progress(f"Filtering by tags (case-insensitive, substring match): {categories}", args.verbose)
        print_progress("Fetching books...", args.verbose)
        # Custom tag filtering: substring match (case-insensitive)
        books = list_calibre_books(conn)
        if categories:
            filtered_books = []
            for book in books:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT t.name FROM tags t
                    JOIN books_tags_link btl ON t.id = btl.tag
                    WHERE btl.book = ?
                """, (book['id'],))
                book_tags = [row[0].lower() for row in cursor.fetchall()]
                # Match if any category is a substring of any tag, or vice versa
                match = False
                for cat in categories:
                    for tag in book_tags:
                        if cat in tag or tag in cat:
                            match = True
                            break
                    if match:
                        break
                if match:
                    filtered_books.append(book)
            books = filtered_books
        print_progress(f"Found {len(books)} books", args.verbose)
        display_books(books, args.output_format, args.output_file, args.library_path, args.verbose, google_creds, args.notice)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
        
if __name__ == '__main__':
    main()
