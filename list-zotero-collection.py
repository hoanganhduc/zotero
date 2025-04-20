#!/usr/bin/env python3
# Standard library imports
import argparse
import concurrent.futures
import json
import os
import html
import io
import sys
from datetime import datetime

# Third-party imports
import pdfkit
import xhtml2pdf
from xhtml2pdf import pisa
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pyzotero import zotero

# Google Drive API imports
# Remark: Create a service account in Google Console and share Zotero folder with the service account email. If you don't share it, you won't be able to access the files.
def authenticate_google_drive(service_account_file):
    """
    Authenticate to Google Drive using a service account.
    
    Args:
        service_account_file (str): Path to the service account key JSON file or JSON string
        
    Returns:
        google.auth.credentials.Credentials: Google API credentials
    """
    
    # Define the scopes required for Google Drive access
    SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
    
    creds = None
    service_info = {}
    
    try:
        # Check if input is a JSON string (starts with '{' and ends with '}')
        if service_account_file.strip().startswith('{') and service_account_file.strip().endswith('}'):
            # Parse JSON string directly
            service_info = json.loads(service_account_file)
            
            # Create credentials from parsed JSON
            service_json_io = io.StringIO(service_account_file)
            creds = service_account.Credentials.from_service_account_info(
                service_info, scopes=SCOPES)
            
            print("Authenticated using provided JSON string")
        else:
            # Treat as file path
            if not os.path.exists(service_account_file):
                print(f"Error: Service account key file not found: {service_account_file}")
                return None
                
            # Read the file and load JSON for getting email
            with open(service_account_file, 'r') as f:
                service_info = json.load(f)
            
            # Create credentials from file
            creds = service_account.Credentials.from_service_account_file(
                service_account_file, scopes=SCOPES)
                
            print(f"Authenticated using service account file: {service_account_file}")
        
        # Get service account email for logging
        service_email = service_info.get('client_email', 'unknown-service-account')
        print(f"Authenticated as service account: {service_email}")
            
    except Exception as e:
        print(f"Error authenticating with service account: {str(e)}")
        return None
    
    return creds

def test_google_drive_access(google_creds, verbose=False):
    """
    Test access to Google Drive using Google credentials.
    
    Args:
        google_creds: Google API credentials object
        verbose (bool): Whether to display verbose output
        
    Returns:
        tuple: (success, message) where success is a boolean indicating if the test was successful,
                and message contains additional information
    """
    if verbose:
        print("Testing Google Drive access using provided credentials")
    
    try:
        if not google_creds:
            return False, "No credentials provided. Authentication failed."
            
        # Build the Drive API client
        drive_service = build('drive', 'v3', credentials=google_creds)
        
        # Try to get account information and file count
        about = drive_service.about().get(fields="user,storageQuota").execute()
        files = drive_service.files().list(
            pageSize=1, 
            fields="files(id,name),nextPageToken"
        ).execute()
        
        # Get service account email from credentials or user info
        service_email = 'Unknown'
        if hasattr(google_creds, 'service_account_email'):
            service_email = google_creds.service_account_email
        elif 'user' in about and 'emailAddress' in about['user']:
            service_email = about['user']['emailAddress']
        
        storage_used = int(about.get('storageQuota', {}).get('usage', 0)) / (1024 * 1024)  # Convert to MB
        storage_total = int(about.get('storageQuota', {}).get('limit', 0)) / (1024 * 1024 * 1024)  # Convert to GB
        
        # Count files (this may take a while for large accounts, so we estimate)
        file_count = "at least 1" if files.get('files') else "0"
        if 'nextPageToken' in files:
            file_count = "more than 100"  # Just an indication that there are many files
            
        # Format the success message
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
    """
    Search for files in Google Drive based on a query.
    
    Args:
        drive_service: Google Drive service instance
        query (str): Search query string
        max_results (int): Maximum number of results to return
        folder_name (str, optional): Name of folder to search within (default: None, searches all of Drive)
        include_shared (bool): Whether to include files shared with the user (default: True)
        
    Returns:
        list: List of file metadata matching the query
    """
    results = []
    page_token = None
    
    # If folder name is specified, find its ID and modify the query
    folder_id = None
    if folder_name:
        # Search for the folder (include both owned and shared folders)
        folder_query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        folder_response = drive_service.files().list(
            q=folder_query,
            spaces='drive',
            fields='files(id, name)',
            pageSize=1).execute()
        
        folders = folder_response.get('files', [])
        if folders:
            folder_id = folders[0]['id']
            # Modify query to search within the specific folder
            query = f"{query} and '{folder_id}' in parents"
    
    # Search in both owned and shared files
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
        
        # If we need to specifically search in shared files and we haven't reached max results
        if include_shared and len(results) < max_results and not folder_id:
            # Create a separate query for shared files
            shared_query = f"{query} and sharedWithMe=true"
            shared_response = drive_service.files().list(
                q=shared_query,
                spaces='drive',
                fields='files(id, name, webViewLink)',
                pageSize=max_results - len(results)
            ).execute()
            
            # Add any unique shared files to results
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
    """
    Find a file in Google Drive by name and return its URL using provided Google credentials.
    
    Args:
        google_creds: Already authenticated Google credentials object
        filename (str): Name of the file to search for
        exact_match (bool): If True, match exact filename, otherwise partial match
        folder_name (str, optional): Name of folder to search within (None searches all of Drive)
        return_all (bool): If True, return all matching files, not just the first one
        verbose (bool): Whether to display progress messages
        
    Returns:
        Union[str, List[str], None]: URL(s) of the file(s) if found, None otherwise
    """
    try:
        if verbose:
            print(f"Searching for file: {filename} in Google Drive")
            
        # Check if credentials are valid
        if not google_creds:
            if verbose:
                print("No valid Google credentials provided")
            return None
            
        # Build the Drive API client
        drive_service = build('drive', 'v3', credentials=google_creds)
        
        # Escape single quotes in filename for query
        safe_filename = filename.replace("'", "\\'")
        
        # Construct the search query based on the filename
        if exact_match:
            query = f"name = '{safe_filename}' and trashed = false"
        else:
            query = f"name contains '{safe_filename}' and trashed = false"
            
        # Search for the file, possibly in a specific folder
        results = search_file_in_drive(drive_service, query, max_results=10 if return_all else 1, folder_name=folder_name)
        
        if verbose:
            print(f"Found {len(results)} matching files")
            
        # Return based on return_all parameter
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
    """
    Print progress messages to track script execution.
    
    Args:
        message (str): The progress message to display
        verbose (bool): Whether to display the message (default: True)
        level (int): Importance level of the message (higher = more important)
        file: File object to write to (default: sys.stdout)
    """
    if verbose:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}", file=file)
        file.flush()  # Ensure the message is written immediately

def connect_to_zotero(library_id, library_type, api_key):
    """Create and return a Zotero connection."""
    return zotero.Zotero(library_id, library_type, api_key)

def list_collections(zot):
    """List all collections in the library."""
    collections = zot.collections()
    return collections

def get_items(zot, collection=None, item_type=None, verbose=True):
    """Retrieve all items based on filters."""
    # Accept verbose parameter and use it for progress messages
    items = []
    if collection:
        print_progress(f"Fetching items from collection {collection}...", verbose)
        items = zot.everything(zot.collection_items(collection))
        print_progress(f"Retrieved {len(items)} items from collection", verbose)
    else:
        if item_type:
            print_progress(f"Fetching items with type '{item_type}'...", verbose)
            items = zot.everything(zot.items(itemType=item_type))
            print_progress(f"Retrieved {len(items)} items of type '{item_type}'", verbose)
        else:
            print_progress("Fetching all library items...", verbose)
            items = zot.everything(zot.items())
            print_progress(f"Retrieved {len(items)} total items", verbose)
    
    # Filter out notes and attachments
    filtered_items = [item for item in items if item['data'].get('itemType') not in ['note', 'attachment']]
    
    # Remove relations field from each item
    for item in filtered_items:
        if 'relations' in item['data']:
            del item['data']['relations']
    print_progress(f"After filtering: {len(filtered_items)} items remain", verbose)
    
    return filtered_items

def get_attachment_paths(zot, item, google_creds=None, verbose=False):
    """
    Get attachment paths for a given item and their Google Drive URLs if available.
    Supports various file types including PDF, DJVU, EPUB, AZW3, MOBI and more.
    
    Args:
        zot: Zotero API client instance
        item: Zotero item to get attachments for
        google_creds: Google API credentials object (already authenticated)
        verbose (bool): Whether to display progress messages
    
    Returns:
        list: A list of dictionaries with keys 'local_path' and 'drive_url' (None if not found)
    """
    if not item:
        return []
    
    # Get the item's attachments
    try:
        attachments = zot.children(item['key'])
    except Exception as e:
        print(f"Error getting attachments for item {item.get('data', {}).get('title', 'Unknown')}: {e}", file=sys.stderr)
        return []
    
    attachment_info = []
    for attachment in attachments:
        # Check if it's an attachment of supported type
        if attachment['data'].get('itemType') == 'attachment' and 'contentType' in attachment['data']:
            content_type = attachment['data']['contentType']
            if content_type in [
                'application/pdf', 
                'image/vnd.djvu',
                'video/mp4',
                'application/vnd.ms-powerpoint',
                'application/vnd.openxmlformats-officedocument.presentationml.presentation',
                # Additional ebook formats
                'application/epub+zip',                   # EPUB
                'application/vnd.amazon.ebook',           # AZW3
                'application/x-mobi8-ebook',              # AZW3 (alternate)
                'application/x-mobipocket-ebook',         # MOBI
                'application/vnd.comicbook+zip',          # CBZ (Comic book)
                'application/x-cbr',                      # CBR (Comic book)
                'application/x-fictionbook+xml',          # FB2
                'text/plain'                              # TXT
            ]:
                # Get the file information
                if 'key' in attachment and 'filename' in attachment['data']:
                    attachment_id = attachment['key']
                    filename = attachment['data']['filename']
                    local_path = f"storage/{attachment_id}/{filename}"
                    
                    # Initialize with local path only
                    info = {'local_path': local_path, 'drive_url': None}
                    
                    # If Google credentials are provided, search in Drive
                    if google_creds:
                        if verbose:
                            print_progress(f"Searching for {filename} in Google Drive", verbose)
                        try:
                            # Search using Google credentials
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

def format_item_text(item, zot, google_creds=None, verbose=False):
    """Format a single item for text output with proper Unicode support."""
    output = []
    # Use Unicode strings for all text content
    output.append(f"Title: {item['data'].get('title', 'Unknown')}")
    output.append(f"Type: {item['data'].get('itemType', 'Unknown')}")
    
    # Format authors
    if 'creators' in item['data'] and item['data']['creators']:
        authors = []
        for creator in item['data']['creators']:
            if 'lastName' in creator and 'firstName' in creator:
                authors.append(f"{creator['lastName']}, {creator['firstName']}")
            elif 'name' in creator:
                authors.append(creator['name'])
        if authors:
            output.append(f"Authors: {'; '.join(authors)}")
    
    if 'date' in item['data'] and item['data']['date']:
        output.append(f"Date: {item['data']['date']}")
    
    # Type-specific fields
    item_type = item['data'].get('itemType')
    if item_type == 'book':
        if 'publisher' in item['data'] and item['data']['publisher']:
            output.append(f"Publisher: {item['data']['publisher']}")
        if 'place' in item['data'] and item['data']['place']:
            output.append(f"Place: {item['data']['place']}")
        if 'ISBN' in item['data'] and item['data']['ISBN']:
            output.append(f"ISBN: {item['data']['ISBN']}")
        # Add DOI for books
        if 'DOI' in item['data'] and item['data']['DOI']:
            output.append(f"DOI: {item['data']['DOI']}")
    elif item_type == 'journalArticle':
        if 'publicationTitle' in item['data'] and item['data']['publicationTitle']:
            output.append(f"Journal: {item['data']['publicationTitle']}")
        if 'volume' in item['data'] and item['data']['volume']:
            output.append(f"Volume: {item['data']['volume']}")
        if 'issue' in item['data'] and item['data']['issue']:
            output.append(f"Issue: {item['data']['issue']}")
        if 'pages' in item['data'] and item['data']['pages']:
            output.append(f"Pages: {item['data']['pages']}")
        if 'DOI' in item['data'] and item['data']['DOI']:
            output.append(f"DOI: {item['data']['DOI']}")
    elif item_type == 'manuscript':
        # Add arXiv URL for manuscripts
        if 'url' in item['data'] and item['data']['url'] and 'arxiv.org' in item['data']['url']:
            output.append(f"arXiv URL: {item['data']['url']}")
        # Check if there's an arXiv ID in extra field
        if 'extra' in item['data'] and item['data']['extra']:
            extra = item['data']['extra']
            if 'arXiv:' in extra:
                for line in extra.split('\n'):
                    if line.strip().startswith('arXiv:'):
                        arxiv_id = line.strip().replace('arXiv:', '').strip()
                        output.append(f"arXiv ID: {arxiv_id}")
                        if 'url' not in item['data'] or 'arxiv.org' not in item['data']['url']:
                            output.append(f"arXiv URL: https://arxiv.org/abs/{arxiv_id}")
    
    # Add DOI for any item type if it exists and hasn't been added yet
    if 'DOI' in item['data'] and item['data']['DOI'] and item_type not in ['book', 'journalArticle']:
        output.append(f"DOI: {item['data']['DOI']}")
    
    # Add attachment paths and Google Drive URLs
    attachments = get_attachment_paths(zot, item, google_creds, verbose)
    if attachments:
        output.append("Attachments:")
        for attachment in attachments:
            local_path = attachment.get('local_path', 'Unknown')
            drive_url = attachment.get('drive_url')
            
            if drive_url:
                output.append(f"  - {local_path} (Drive: {drive_url})")
            else:
                output.append(f"  - {local_path}")
    
    # Join all lines with Unicode newlines and ensure the result is Unicode
    return "\n".join(output)

def format_item_html(item, zot, google_creds=None, verbose=False):
    """Format a single item for HTML output."""
    # Use html.escape for all text content to handle Unicode properly
    
    # Start with basic item info
    html_parts = [f"<div class='item {html.escape(item['data'].get('itemType', ''))}'>"
            f"<h2>{html.escape(item['data'].get('title', 'Unknown'))}</h2>"]
    
    html_parts.append(f"<p><strong>Type:</strong> {html.escape(item['data'].get('itemType', 'Unknown'))}</p>")
    
    # Format authors
    if 'creators' in item['data'] and item['data']['creators']:
        authors = []
        for creator in item['data']['creators']:
            if 'lastName' in creator and 'firstName' in creator:
                authors.append(f"{html.escape(creator['lastName'])}, {html.escape(creator['firstName'])}")
            elif 'name' in creator:
                authors.append(html.escape(creator['name']))
        if authors:
            html_parts.append(f"<p><strong>Authors:</strong> {html.escape('; '.join(authors))}</p>")
    
    if 'date' in item['data'] and item['data']['date']:
        html_parts.append(f"<p><strong>Date:</strong> {html.escape(item['data']['date'])}</p>")
    
    # Type-specific fields
    item_type = item['data'].get('itemType')
    if item_type == 'book':
        if 'publisher' in item['data'] and item['data']['publisher']:
            html_parts.append(f"<p><strong>Publisher:</strong> {html.escape(item['data']['publisher'])}</p>")
        if 'place' in item['data'] and item['data']['place']:
            html_parts.append(f"<p><strong>Place:</strong> {html.escape(item['data']['place'])}</p>")
        if 'ISBN' in item['data'] and item['data']['ISBN']:
            html_parts.append(f"<p><strong>ISBN:</strong> {html.escape(item['data']['ISBN'])}</p>")
        # Add DOI for books
        if 'DOI' in item['data'] and item['data']['DOI']:
            html_parts.append(f"<p><strong>DOI:</strong> {html.escape(item['data']['DOI'])}</p>")
    elif item_type == 'journalArticle':
        if 'publicationTitle' in item['data'] and item['data']['publicationTitle']:
            html_parts.append(f"<p><strong>Journal:</strong> {html.escape(item['data']['publicationTitle'])}</p>")
        if 'volume' in item['data'] and item['data']['volume']:
            html_parts.append(f"<p><strong>Volume:</strong> {html.escape(item['data']['volume'])}</p>")
        if 'issue' in item['data'] and item['data']['issue']:
            html_parts.append(f"<p><strong>Issue:</strong> {html.escape(item['data']['issue'])}</p>")
        if 'pages' in item['data'] and item['data']['pages']:
            html_parts.append(f"<p><strong>Pages:</strong> {html.escape(item['data']['pages'])}</p>")
        if 'DOI' in item['data'] and item['data']['DOI']:
            html_parts.append(f"<p><strong>DOI:</strong> {html.escape(item['data']['DOI'])}</p>")
    elif item_type == 'manuscript':
        # Add arXiv URL for manuscripts
        if 'url' in item['data'] and item['data']['url'] and 'arxiv.org' in item['data']['url']:
            html_parts.append(f"<p><strong>arXiv URL:</strong> <a href='{html.escape(item['data']['url'])}' target='_blank'>{html.escape(item['data']['url'])}</a></p>")
        # Check if there's an arXiv ID in extra field
        if 'extra' in item['data'] and item['data']['extra']:
            extra = item['data']['extra']
            if 'arXiv:' in extra:
                for line in extra.split('\n'):
                    if line.strip().startswith('arXiv:'):
                        arxiv_id = line.strip().replace('arXiv:', '').strip()
                        html_parts.append(f"<p><strong>arXiv ID:</strong> {html.escape(arxiv_id)}</p>")
                        if 'url' not in item['data'] or 'arxiv.org' not in item['data']['url']:
                            arxiv_url = f"https://arxiv.org/abs/{arxiv_id}"
                            html_parts.append(f"<p><strong>arXiv URL:</strong> <a href='{html.escape(arxiv_url)}' target='_blank'>{html.escape(arxiv_url)}</a></p>")
    
    # Add DOI for any item type if it exists and hasn't been added yet
    if 'DOI' in item['data'] and item['data']['DOI'] and item_type not in ['book', 'journalArticle']:
        html_parts.append(f"<p><strong>DOI:</strong> {html.escape(item['data']['DOI'])}</p>")
    
    # Add attachment paths with Google Drive links
    attachments = get_attachment_paths(zot, item, google_creds, verbose)
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

def generate_text_output(items, zot, collection_name=None, google_creds=None, verbose=False):
    """Generate complete text document from items with proper Unicode support."""
    
    if verbose:
        print_progress("Starting text output generation", verbose)
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    title = f"Zotero Items - {current_date}"
    if collection_name:
        title = f"Zotero Collection: {collection_name} - {current_date}"
        
    header = [
        title,
        "=" * len(title),
        ""  # Empty line after header
    ]
    
    if verbose:
        print_progress(f"Preparing to format {len(items)} items simultaneously", verbose)
    
    # Helper function to format a single item
    def format_single_item(idx, item):
        try:
            item_header = f"{collection_name} #{idx+1}"
            item_content = format_item_text(item, zot, google_creds, verbose)
            return f"{item_header}\n{item_content}\n---"
        except Exception as e:
            error_msg = f"Error formatting item {idx+1}: {e}"
            print_progress(error_msg, verbose, file=sys.stderr)
            return f"{error_msg}\n---"
    
    # Process items in parallel with ThreadPoolExecutor
    formatted_items = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Create and submit all tasks
        future_to_idx = {}
        for i, item in enumerate(items):
            future = executor.submit(format_single_item, i, item)
            future_to_idx[future] = i
        
        # Process results as they complete
        completed = 0
        for future in concurrent.futures.as_completed(future_to_idx):
            completed += 1
            if verbose and (completed % 10 == 0 or completed == len(items)):
                print_progress(f"Completed {completed}/{len(items)} items", verbose)
            
            # Store results with their index for later sorting
            idx = future_to_idx[future]
            try:
                formatted_items.append((idx, future.result()))
            except Exception as e:
                error_msg = f"Error processing item {idx+1}: {e}"
                print_progress(error_msg, verbose, file=sys.stderr)
                formatted_items.append((idx, f"{error_msg}\n---"))
    
    # Sort by original index to maintain order
    formatted_items.sort(key=lambda x: x[0])
    ordered_items = [text for _, text in formatted_items]
    
    if verbose:
        print_progress("Text output generation complete", verbose)
    
    # Ensure Unicode output    
    return "\n".join(header + ordered_items)

def generate_html_header(title):
    """Generate the HTML header section with styles and KaTeX support."""
    return [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        f"<title>{title}</title>",
        "<!-- KaTeX CSS -->",
        "<link rel='stylesheet' href='https://cdn.jsdelivr.net/npm/katex@0.16.8/dist/katex.min.css' integrity='sha384-GvrOXuhMATgEsSwCs4smul74iXGOixntILdUW9XmUC6+HX0sLNAK3q71HotJqlAn' crossorigin='anonymous'>",
        "<!-- KaTeX JS -->",
        "<script defer src='https://cdn.jsdelivr.net/npm/katex@0.16.8/dist/katex.min.js' integrity='sha384-cpW21h6RZv/phavutF+AuVYrr+dA8xD9zs6FwLpaCct6O9ctzYFfFr4dgmgccOTx' crossorigin='anonymous'></script>",
        "<!-- KaTeX auto-render extension -->",
        "<script defer src='https://cdn.jsdelivr.net/npm/katex@0.16.8/dist/contrib/auto-render.min.js' integrity='sha384-+VBxd3r6XgURycqtZ117nYw44OOcIax56Z4dCRWbxyPt0Koah1uHoK0o4+/RRE05' crossorigin='anonymous'></script>",
        "<script>",
        "document.addEventListener('DOMContentLoaded', function() {",
        "  renderMathInElement(document.body, {",
        "    delimiters: [",
        "      {left: '$$', right: '$$', display: true},",
        "      {left: '$', right: '$', display: false}",
        "    ],",
        "    throwOnError: false",
        "  });",
        "});",
        "</script>",
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
        "<div class='notice'>This page is created from a Zotero collection of <a href='https://hoanganhduc.github.io/'>Duc A. Hoang</a> using the <a href='list-zotero-collection.py'>list-zotero-collection.py</a> script. Materials listed have been gathered from various sources. Access to these materials via Google Drive is restricted due to possible copyright issues.</div>"
    ]

def generate_search_container():
    """Generate the search box HTML."""
    return [
        "<div class='search-container'>",
        "<input type='text' id='searchInput' placeholder='Search within this page...' />",
        "<button id='searchBtn'>Search</button>",
        "<span id='searchCount'></span>",
        "</div>"
    ]

def format_single_item(idx, item, collection_name, zot, google_creds, verbose):
    """Format a single item for HTML output."""
    try:
        item_number = f"<div class='item-number'>{collection_name} #{idx+1}</div>"
        item_content = format_item_html(item, zot, google_creds, verbose)
        return item_number + "\n" + item_content
    except Exception as e:
        error_msg = f"Error formatting item {idx+1}: {e}"
        print_progress(error_msg, verbose, file=sys.stderr)
        return f"<div class='item-error'>{error_msg}</div>"

def generate_items_html(items, collection_name, zot, google_creds, verbose):
    """Generate HTML for all items using parallel processing."""
    if verbose:
        print_progress(f"Preparing to format {len(items)} items simultaneously", verbose)
    
    # Process items in parallel with ThreadPoolExecutor
    formatted_items = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Create and submit all tasks
        future_to_idx = {}
        for i, item in enumerate(items):
            future = executor.submit(format_single_item, i, item, collection_name, zot, google_creds, verbose)
            future_to_idx[future] = i
        
        # Process results as they complete
        completed = 0
        for future in concurrent.futures.as_completed(future_to_idx):
            completed += 1
            if verbose and (completed % 10 == 0 or completed == len(items)):
                print_progress(f"Completed {completed}/{len(items)} items", verbose)
            
            # Store results with their index for later sorting
            idx = future_to_idx[future]
            try:
                formatted_items.append((idx, future.result()))
            except Exception as e:
                error_msg = f"Error processing item {idx+1}: {e}"
                print_progress(error_msg, verbose, file=sys.stderr)
                formatted_items.append((idx, f"<div class='item-error'>{error_msg}</div>"))
    
    # Sort by original index to maintain order
    formatted_items.sort(key=lambda x: x[0])
    return [html_content for _, html_content in formatted_items]

def generate_search_script():
    """Generate the JavaScript code for search functionality."""
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
        "      // Show all items if search is empty",
        "      items.forEach(item => {",
        "        item.classList.remove('hidden');",
        "        // Remove any existing highlights",
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
        "",
        "    let matchCount = 0;",
        "",
        "    // Process each item",
        "    items.forEach(item => {",
        "      const text = item.textContent.toLowerCase();",
        "      const hasMatch = text.includes(searchTerm);",
        "      ",
        "      // Show/hide based on match",
        "      if (hasMatch) {",
        "        item.classList.remove('hidden');",
        "        matchCount++;",
        "        ",
        "        // Highlight matches (only in text nodes)",
        "        highlightText(item, searchTerm);",
        "      } else {",
        "        item.classList.add('hidden');",
        "      }",
        "    });",
        "",
        "    // Update count display",
        "    searchCount.textContent = `Found ${matchCount} matching items`;",
        "  }",
        "",
        "  function highlightText(element, searchTerm) {",
        "    // Remove any existing highlights first",
        "    const highlighted = element.querySelectorAll('.highlight');",
        "    highlighted.forEach(el => {",
        "      const parent = el.parentNode;",
        "      parent.replaceChild(document.createTextNode(el.textContent), el);",
        "      parent.normalize();",
        "    });",
        "",
        "    // Function to recursively process text nodes",
        "    function processNode(node) {",
        "      // Only process text nodes",
        "      if (node.nodeType === 3) {",
        "        const text = node.nodeValue.toLowerCase();",
        "        const index = text.indexOf(searchTerm.toLowerCase());",
        "        ",
        "        // If search term found in this text node",
        "        if (index >= 0) {",
        "          const before = node.nodeValue.substring(0, index);",
        "          const match = node.nodeValue.substring(index, index + searchTerm.length);",
        "          const after = node.nodeValue.substring(index + searchTerm.length);",
        "          ",
        "          const beforeNode = document.createTextNode(before);",
        "          const matchNode = document.createElement('span');",
        "          matchNode.classList.add('highlight');",
        "          matchNode.textContent = match;",
        "          const afterNode = document.createTextNode(after);",
        "          ",
        "          const parent = node.parentNode;",
        "          parent.replaceChild(afterNode, node);",
        "          parent.insertBefore(matchNode, afterNode);",
        "          parent.insertBefore(beforeNode, matchNode);",
        "          ",
        "          // Process the 'after' part too for multiple occurrences",
        "          processNode(afterNode);",
        "        }",
        "      } else if (node.nodeType === 1 && node.childNodes && !/(script|style)/i.test(node.tagName)) {",
        "        // Process children of element nodes",
        "        Array.from(node.childNodes).forEach(child => processNode(child));",
        "      }",
        "    }",
        "",
        "    // Start processing from the item element",
        "    processNode(element);",
        "  }",
        "",
        "  // Event listeners",
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

def generate_html_output(items, zot, collection_name=None, google_creds=None, verbose=False):
    """Generate complete HTML document from items."""
    if verbose:
        print_progress("Starting HTML output generation", verbose)
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    title = f"Zotero Items - {current_date}"
    if collection_name:
        title = f"Zotero Collection: {collection_name} - {current_date}"
    
    # Build HTML components
    html_parts = []
    html_parts.extend(generate_html_header(title))
    html_parts.extend(generate_search_container())
    
    # Process items
    formatted_items = generate_items_html(items, collection_name, zot, google_creds, verbose)
    html_parts.extend(formatted_items)
    
    # Add search functionality
    html_parts.extend(generate_search_script())
    
    if verbose:
        print_progress("HTML output generation complete", verbose)
    
    return "\n".join(html_parts)

# For PDF generation
try:
    PDF_GENERATOR_AVAILABLE = True
    PDF_GENERATOR_NAME = "pdfkit"
except ImportError:
    try:
        PDF_GENERATOR_AVAILABLE = True
        PDF_GENERATOR_NAME = "xhtml2pdf"
    except ImportError:
        PDF_GENERATOR_AVAILABLE = False
        PDF_GENERATOR_NAME = None

def generate_pdf_output(html_content, output_file, verbose=False):
    """Generate PDF from HTML content using pdfkit or xhtml2pdf as fallback."""
    if not PDF_GENERATOR_AVAILABLE:
        print("Error: No PDF generation library available. Cannot generate PDF.", file=sys.stderr)
        print("Please install either pdfkit or xhtml2pdf:", file=sys.stderr)
        print("  pip install pdfkit  # Recommended (requires wkhtmltopdf binary)", file=sys.stderr)
        print("  pip install xhtml2pdf  # Pure Python alternative", file=sys.stderr)
        sys.exit(1)
    
    if verbose:
        print_progress("Starting PDF generation...", verbose)
        html_size_kb = len(html_content) / 1024
        print_progress(f"Using {PDF_GENERATOR_NAME} to process approximately {html_size_kb:.1f} KB of HTML content", verbose)
    
    try:
        # Generate the PDF based on available library
        if PDF_GENERATOR_NAME == "pdfkit":
            # Configure pdfkit options if needed
            options = {
                'quiet': not verbose,
                'encoding': "UTF-8",
            }
            pdfkit.from_string(html_content, output_file, options=options)
        else:  # xhtml2pdf
            with open(output_file, "wb") as pdf_file:
                pisa_status = pisa.CreatePDF(
                    html_content,
                    dest=pdf_file
                )
            if pisa_status.err:
                raise Exception("xhtml2pdf encountered errors during conversion")
        
        # Get the file size of the generated PDF
        if os.path.exists(output_file):
            pdf_size_kb = os.path.getsize(output_file) / 1024
            print_progress(f"PDF successfully generated ({pdf_size_kb:.1f} KB) and saved to {output_file}", verbose)
        else:
            print_progress("PDF generation seemed to complete but output file not found", verbose, file=sys.stderr)
    
    except Exception as e:
        print_progress(f"Error generating PDF with {PDF_GENERATOR_NAME}: {str(e)}", verbose, file=sys.stderr)
        sys.exit(1)

def display_collections(collections, output_format, output_file=None, verbose=False):
    """Display collections in the specified format."""
    if not collections:
        print("No collections found.")
        return
    
    print_progress("Displaying collections...", verbose)
    
    if output_format == 'text':
        print_progress(f"Formatting {len(collections)} collections as text", verbose)
        for i, collection in enumerate(collections):
            if verbose and (i % 10 == 0 or i == len(collections) - 1):
                print_progress(f"Processing collection {i+1} of {len(collections)}", verbose)
            print(f"Name: {collection['data']['name']}")
            print(f"Key: {collection['data']['key']}")
            print("---")
    elif output_format in ['html', 'pdf']:
        print_progress(f"Formatting {len(collections)} collections as HTML", verbose)
        html = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "<title>Zotero Collections</title>",
            "<style>",
            "body { font-family: Arial, sans-serif; margin: 40px; }",
            ".collection { margin-bottom: 20px; }",
            "h1 { color: #2c3e50; }",
            "</style>",
            "</head>",
            "<body>",
            "<h1>Zotero Collections</h1>"
        ]
        
        for i, collection in enumerate(collections):
            if verbose and (i % 10 == 0 or i == len(collections) - 1):
                print_progress(f"Processing collection {i+1} of {len(collections)}", verbose)
            html.append("<div class='collection'>")
            html.append(f"<p><strong>Name:</strong> {collection['data']['name']}</p>")
            html.append(f"<p><strong>Key:</strong> {collection['data']['key']}</p>")
            html.append("</div>")
        
        html.extend(["</body>", "</html>"])
        html_content = "\n".join(html)
        
        if output_format == 'html':
            if output_file:
                print_progress(f"Saving HTML output to {output_file}", verbose)
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                print(f"HTML output saved to {output_file}")
            else:
                print_progress("Displaying HTML output", verbose)
                print(html_content)
        else:  # pdf
            if not output_file:
                output_file = "zotero_collections.pdf"
            print_progress(f"Generating PDF output to {output_file}", verbose)
            generate_pdf_output(html_content, output_file, verbose)
            print(f"PDF output saved to {output_file}")
    
    print_progress("Collection display complete", verbose)

def display_items(items, output_format, output_file=None, collection_name=None, zot=None, verbose=False, google_creds=None):
    """Display items in the specified format."""
    if not items:
        print("No items found.")
        return
    
    print_progress("Displaying items...", verbose)
    
    if output_format == 'text':
        print_progress("Generating text output...", verbose)
        text_content = generate_text_output(items, zot, collection_name, google_creds, verbose)
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
        html_content = generate_html_output(items, zot, collection_name, google_creds, verbose)
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
        html_content = generate_html_output(items, zot, collection_name, google_creds, verbose)
        if not output_file:
            output_file = "zotero_items.pdf"
            print_progress(f"No output file specified, using default: {output_file}", verbose)
        generate_pdf_output(html_content, output_file, verbose)
        print(f"PDF output saved to {output_file}")
    
    print_progress("Item display complete", verbose)

def parse_arguments():
    """Parse and return command line arguments."""
    parser = argparse.ArgumentParser(description='List items from a Zotero collection.')
    parser.add_argument('-k', '--api-key', required=True, help='Your Zotero API key')
    parser.add_argument('-t', '--library-type', choices=['user', 'group'], default='user',
                        help='Type of library (user or group)')
    parser.add_argument('-l', '--library-id', required=True, help='Your user or group ID')
    parser.add_argument('-c', '--collection', help='Collection ID (optional)')
    parser.add_argument('-i', '--item-type', help='Filter by item type (e.g., book, journalArticle)')
    parser.add_argument('-L', '--list-collections', action='store_true', 
                        help='List all collections instead of items')
    parser.add_argument('-o', '--output-format', choices=['text', 'html', 'pdf'], default='text',
                        help='Output format (default: text)')
    parser.add_argument('-f', '--output-file', help='Output file name (for html and pdf)')
    parser.add_argument('-v', '--verbose', action='store_true', 
                        help='Display progress information during execution')
    parser.add_argument('-s', '--service-account-file', 
                        help='Path to Google service account JSON file or the JSON string itself for Drive integration')
    
    return parser.parse_args()

def handle_collection_listing(zot, output_format, output_file, verbose):
    """Handle the workflow for listing collections."""
    print_progress("Fetching collections...", verbose)
    collections = list_collections(zot)
    print_progress(f"Found {len(collections)} collections", verbose)
    display_collections(collections, output_format, output_file, verbose)

def get_collection_name(zot, collection_id, verbose):
    """Get the name of a collection given its ID."""
    collection_name = None
    if collection_id:
        try:
            print_progress(f"Getting information for collection {collection_id}...", verbose)
            collection = zot.collection(collection_id)
            collection_name = collection.get('data', {}).get('name')
            if collection_name:
                print_progress(f"Collection name: {collection_name}", verbose)
        except Exception as e:
            print_progress(f"Could not retrieve collection name: {e}", verbose, file=sys.stderr)
    
    return collection_name

def handle_item_listing(zot, collection_id, item_type, output_format, output_file, verbose, 
                       google_creds=None):
    """Handle the workflow for listing items."""
    print_progress("Fetching items...", verbose)
    items = get_items(zot, collection_id, item_type, verbose)
    print_progress(f"Found {len(items)} items", verbose)
    
    # Get collection name if a collection ID was provided
    collection_name = get_collection_name(zot, collection_id, verbose)
    
    print_progress(f"Generating {output_format} output...", verbose)
    display_items(items, output_format, output_file, collection_name, zot, verbose, 
                 google_creds)
    print_progress("Output generation complete", verbose)

def main():
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Connect to Zotero
        print_progress("Connecting to Zotero...", args.verbose)
        zot = connect_to_zotero(args.library_id, args.library_type, args.api_key)
        print_progress("Connection established successfully", args.verbose)

        # Set up Google Drive credentials if service account file was provided
        google_creds = None
        if args.service_account_file:
            print_progress("Authenticating with Google Drive using service account...", args.verbose)
            google_creds = authenticate_google_drive(args.service_account_file)
            
            # Test Google Drive access with the credentials
            if google_creds:
                print_progress("Testing Google Drive access...", args.verbose)
                success, message = test_google_drive_access(google_creds, verbose=args.verbose)
                
                if success:
                    print_progress("Google Drive access verified successfully!", args.verbose)
                    print_progress(message, args.verbose)
                else:
                    print_progress("Google Drive access failed!", args.verbose, level=3, file=sys.stderr)
                    print_progress(message, args.verbose, file=sys.stderr)
                    print_progress("The script will continue, but Google Drive links won't be available.", args.verbose)
            else:
                print_progress("Google Drive authentication failed. Google Drive integration will be disabled.", args.verbose, file=sys.stderr)
        else:
            print_progress("No Google Drive service account file provided. Google Drive integration will be disabled.", args.verbose)
        
        # List collections or items
        if args.list_collections:
            handle_collection_listing(zot, args.output_format, args.output_file, args.verbose)
        else:
            handle_item_listing(zot, args.collection, args.item_type, 
                               args.output_format, args.output_file, args.verbose,
                               google_creds)
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()