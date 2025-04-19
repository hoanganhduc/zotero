#!/usr/bin/env python3
from pyzotero import zotero
import argparse
import sys
from datetime import datetime
import os
import concurrent.futures
import pickle
import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Google Drive API imports

def authenticate_google_drive(username, app_password):
    """
    Authenticate to Google Drive using username and app password.
    
    Args:
        username (str): Google account username (email)
        app_password (str): Google app password
        
    Returns:
        google.auth.credentials.Credentials: Google API credentials
    """
    # Define the scopes required for Google Drive access
    SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
    
    creds = None
    # The file token.pickle stores the user's access and refresh tokens
    token_file = f"token_{username.split('@')[0]}.pickle"
    
    if os.path.exists(token_file):
        with open(token_file, 'rb') as token:
            creds = pickle.load(token)
    
    # If there are no valid credentials available, use the app password
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # For first-time setup, you need credentials.json from Google Cloud Console
            if not os.path.exists('credentials.json'):
                print("Error: credentials.json file missing. Please download it from Google Cloud Console.")
                return None
                
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            print(f"Please sign in with your Google account: {username}")
            print(f"Use your app password when prompted.")
            creds = flow.run_local_server(port=0)
            
        # Save the credentials for the next run
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
    
    return creds

def search_file_in_drive(drive_service, query, max_results=10, folder_name=None):
    """
    Search for files in Google Drive based on a query.
    
    Args:
        drive_service: Google Drive service instance
        query (str): Search query string
        max_results (int): Maximum number of results to return
        folder_name (str, optional): Name of folder to search within (default: None, searches all of Drive)
        
    Returns:
        list: List of file metadata matching the query
    """
    results = []
    page_token = None
    
    # If folder name is specified, find its ID and modify the query
    folder_id = None
    if folder_name:
        # Search for the folder
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
    
    # Perform the search
    while True:
        response = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='nextPageToken, files(id, name, webViewLink)',
            pageToken=page_token,
            pageSize=max_results).execute()
            
        results.extend(response.get('files', []))
        page_token = response.get('nextPageToken', None)
        
        if page_token is None or len(results) >= max_results:
            break
            
    return results[:max_results]

def get_drive_url_by_filename(username, app_password, filename, exact_match=True, folder_name=None, return_all=False, verbose=False):
    """
    Find a file in Google Drive by name and return its URL.
    
    Args:
        username (str): Google account username
        app_password (str): Google app password
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
            
        # Authenticate to Google Drive
        creds = authenticate_google_drive(username, app_password)
        if not creds:
            if verbose:
                print("Authentication failed")
            return None
            
        # Build the Drive API client
        drive_service = build('drive', 'v3', credentials=creds)
        
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

# For PDF generation
try:
    from weasyprint import HTML
    WEASYPRINT_AVAILABLE = True
except ImportError:
    WEASYPRINT_AVAILABLE = False

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

def get_attachment_paths(zot, item, google_username=None, google_app_password=None, verbose=False):
    """
    Get PDF or DJVU attachment paths for a given item and their Google Drive URLs if available.
    
    Args:
        zot: Zotero API client instance
        item: Zotero item to get attachments for
        google_username (str, optional): Google account username for Drive search
        google_app_password (str, optional): Google app password for authentication
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
        # Check if it's a PDF or DJVU attachment
        if attachment['data'].get('itemType') == 'attachment' and 'contentType' in attachment['data']:
            content_type = attachment['data']['contentType']
            if content_type in ['application/pdf', 'image/vnd.djvu']:
                # Get the file information
                if 'key' in attachment and 'filename' in attachment['data']:
                    attachment_id = attachment['key']
                    filename = attachment['data']['filename']
                    local_path = f"storage/{attachment_id}/{filename}"
                    
                    # Initialize with local path only
                    info = {'local_path': local_path, 'drive_url': None}
                    
                    # If Google credentials are provided, search in Drive
                    if google_username and google_app_password:
                        if verbose:
                            print_progress(f"Searching for {filename} in Google Drive", verbose)
                        try:
                            # Search specifically in the Zotero folder
                            drive_url = get_drive_url_by_filename(
                                google_username, 
                                google_app_password, 
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

def format_item_text(item, zot, google_username=None, google_app_password=None, verbose=False):
    """Format a single item for text output."""
    output = []
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
    
    # Add attachment paths and Google Drive URLs
    attachments = get_attachment_paths(zot, item, google_username, google_app_password, verbose)
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

def format_item_html(item, zot, google_username=None, google_app_password=None, verbose=False):
    """Format a single item for HTML output."""
    html = [f"<div class='item {item['data'].get('itemType', '')}'>"
            f"<h2>{item['data'].get('title', 'Unknown')}</h2>"]
    
    html.append(f"<p><strong>Type:</strong> {item['data'].get('itemType', 'Unknown')}</p>")
    
    # Format authors
    if 'creators' in item['data'] and item['data']['creators']:
        authors = []
        for creator in item['data']['creators']:
            if 'lastName' in creator and 'firstName' in creator:
                authors.append(f"{creator['lastName']}, {creator['firstName']}")
            elif 'name' in creator:
                authors.append(creator['name'])
        if authors:
            html.append(f"<p><strong>Authors:</strong> {'; '.join(authors)}</p>")
    
    if 'date' in item['data'] and item['data']['date']:
        html.append(f"<p><strong>Date:</strong> {item['data']['date']}</p>")
    
    # Type-specific fields
    item_type = item['data'].get('itemType')
    if item_type == 'book':
        if 'publisher' in item['data'] and item['data']['publisher']:
            html.append(f"<p><strong>Publisher:</strong> {item['data']['publisher']}</p>")
        if 'place' in item['data'] and item['data']['place']:
            html.append(f"<p><strong>Place:</strong> {item['data']['place']}</p>")
        if 'ISBN' in item['data'] and item['data']['ISBN']:
            html.append(f"<p><strong>ISBN:</strong> {item['data']['ISBN']}</p>")
    elif item_type == 'journalArticle':
        if 'publicationTitle' in item['data'] and item['data']['publicationTitle']:
            html.append(f"<p><strong>Journal:</strong> {item['data']['publicationTitle']}</p>")
        if 'volume' in item['data'] and item['data']['volume']:
            html.append(f"<p><strong>Volume:</strong> {item['data']['volume']}</p>")
        if 'issue' in item['data'] and item['data']['issue']:
            html.append(f"<p><strong>Issue:</strong> {item['data']['issue']}</p>")
        if 'pages' in item['data'] and item['data']['pages']:
            html.append(f"<p><strong>Pages:</strong> {item['data']['pages']}</p>")
        if 'DOI' in item['data'] and item['data']['DOI']:
            html.append(f"<p><strong>DOI:</strong> {item['data']['DOI']}</p>")
    
    # Add attachment paths with Google Drive links
    attachments = get_attachment_paths(zot, item, google_username, google_app_password, verbose)
    if attachments:
        html.append("<p><strong>Attachments:</strong></p>")
        html.append("<ul>")
        for attachment in attachments:
            local_path = attachment.get('local_path', 'Unknown')
            drive_url = attachment.get('drive_url')
            
            if drive_url:
                html.append(f"<li>{local_path} - <a href='{drive_url}' target='_blank'>View on Google Drive</a></li>")
            else:
                html.append(f"<li>{local_path}</li>")
        html.append("</ul>")
    
    html.append("</div>")
    return "\n".join(html)

def generate_text_output(items, zot, collection_name=None, google_username=None, google_app_password=None, verbose=False):
    """Generate complete text document from items."""
    
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
            item_content = format_item_text(item, zot, google_username, google_app_password, verbose)
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
        
    return "\n".join(header + ordered_items)

def generate_html_output(items, zot, collection_name=None, google_username=None, google_app_password=None, verbose=False):
    """Generate complete HTML document from items."""
    if verbose:
        print_progress("Starting HTML output generation", verbose)
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    title = f"Zotero Items - {current_date}"
    if collection_name:
        title = f"Zotero Collection: {collection_name} - {current_date}"
        
    html = [
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
        "</style>",
        "</head>",
        "<body>",
        "<div class='coffee-button'>",
        "<a href='https://www.buymeacoffee.com/hoanganhduc' target='_blank'>",
        "<img src='https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png' alt='Buy Me A Coffee'>",
        "</a>",
        "</div>",
        f"<h1>{title}</h1>",
        "<div class='notice'>This list is created from a Zotero collection of <a href='https://hoanganhduc.github.io/'>Duc A. Hoang</a>. Please note that access to Google Drive URLs is restricted.</div>"
    ]
    
    if verbose:
        print_progress(f"Preparing to format {len(items)} items simultaneously", verbose)
    
    # Helper function to format a single item
    def format_single_item(idx, item):
        try:
            item_number = f"<div class='item-number'>{collection_name} #{idx+1}</div>"
            item_content = format_item_html(item, zot, google_username, google_app_password, verbose)
            return item_number + "\n" + item_content
        except Exception as e:
            error_msg = f"Error formatting item {idx+1}: {e}"
            print_progress(error_msg, verbose, file=sys.stderr)
            return f"<div class='item-error'>{error_msg}</div>"
    
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
                formatted_items.append((idx, f"<div class='item-error'>{error_msg}</div>"))
    
    # Sort by original index to maintain order
    formatted_items.sort(key=lambda x: x[0])
    ordered_items = [html_content for _, html_content in formatted_items]
    
    # Add the formatted items to the HTML content
    html.extend(ordered_items)
    html.extend(["</body>", "</html>"])
    
    if verbose:
        print_progress("HTML output generation complete", verbose)
    
    return "\n".join(html)

def generate_pdf_output(html_content, output_file, verbose=False):
    """Generate PDF from HTML content with improved error handling."""
    if not WEASYPRINT_AVAILABLE:
        print("Error: WeasyPrint library not available. Cannot generate PDF.", file=sys.stderr)
        print("Please install it with: pip install weasyprint", file=sys.stderr)
        sys.exit(1)
    
    if verbose:
        print_progress("Starting PDF generation...", verbose)
        # Provide an estimate of the size of the HTML content
        html_size_kb = len(html_content) / 1024
        print_progress(f"Processing approximately {html_size_kb:.1f} KB of HTML content", verbose)
    
    try:
        # Generate the PDF
        HTML(string=html_content).write_pdf(output_file)
        
        if verbose:
            # Get the file size of the generated PDF
            pdf_size_kb = os.path.getsize(output_file) / 1024
            print_progress(f"PDF successfully generated ({pdf_size_kb:.1f} KB) and saved to {output_file}", verbose)
    
    except Exception as e:
        print_progress(f"Error generating PDF: {str(e)}", verbose, file=sys.stderr)
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
                with open(output_file, 'w') as f:
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

def display_items(items, output_format, output_file=None, collection_name=None, zot=None, verbose=False, google_username=None, google_app_password=None):
    """Display items in the specified format."""
    if not items:
        print("No items found.")
        return
    
    print_progress("Displaying items...", verbose)
    
    if output_format == 'text':
        print_progress("Generating text output...", verbose)
        text_content = generate_text_output(items, zot, collection_name, google_username, google_app_password, verbose)
        if output_file:
            print_progress(f"Saving text output to {output_file}", verbose)
            with open(output_file, 'w') as f:
                f.write(text_content)
            print(f"Text output saved to {output_file}")
        else:
            print_progress("Displaying text output to console", verbose)
            print(text_content)
    elif output_format == 'html':
        print_progress("Generating HTML output...", verbose)
        html_content = generate_html_output(items, zot, collection_name, google_username, google_app_password, verbose)
        if output_file:
            print_progress(f"Saving HTML output to {output_file}", verbose)
            with open(output_file, 'w') as f:
                f.write(html_content)
            print(f"HTML output saved to {output_file}")
        else:
            print_progress("Displaying HTML output to console", verbose)
            print(html_content)
    elif output_format == 'pdf':
        print_progress("Generating PDF output...", verbose)
        html_content = generate_html_output(items, zot, collection_name, google_username, google_app_password, verbose)
        if not output_file:
            output_file = "zotero_items.pdf"
            print_progress(f"No output file specified, using default: {output_file}", verbose)
        generate_pdf_output(html_content, output_file, verbose)
        print(f"PDF output saved to {output_file}")
    
    print_progress("Item display complete", verbose)

def parse_arguments():
    """Parse and return command line arguments."""
    parser = argparse.ArgumentParser(description='List items from a Zotero collection.')
    parser.add_argument('--api-key', required=True, help='Your Zotero API key')
    parser.add_argument('--library-type', choices=['user', 'group'], default='user',
                        help='Type of library (user or group)')
    parser.add_argument('--library-id', required=True, help='Your user or group ID')
    parser.add_argument('--collection', help='Collection ID (optional)')
    parser.add_argument('--item-type', help='Filter by item type (e.g., book, journalArticle)')
    parser.add_argument('--list-collections', action='store_true', 
                        help='List all collections instead of items')
    parser.add_argument('--output-format', choices=['text', 'html', 'pdf'], default='text',
                        help='Output format (default: text)')
    parser.add_argument('--output-file', help='Output file name (for html and pdf)')
    parser.add_argument('--verbose', action='store_true', 
                        help='Display progress information during execution')
    parser.add_argument('--google-username', help='Google account username for Drive integration')
    parser.add_argument('--google-app-password', help='Google app password for Drive integration')
    
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
                       google_username=None, google_app_password=None):
    """Handle the workflow for listing items."""
    print_progress("Fetching items...", verbose)
    items = get_items(zot, collection_id, item_type, verbose)
    print_progress(f"Found {len(items)} items", verbose)
    
    # Get collection name if a collection ID was provided
    collection_name = get_collection_name(zot, collection_id, verbose)
    
    print_progress(f"Generating {output_format} output...", verbose)
    display_items(items, output_format, output_file, collection_name, zot, verbose, 
                 google_username, google_app_password)
    print_progress("Output generation complete", verbose)

def main():
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Connect to Zotero
        print_progress("Connecting to Zotero...", args.verbose)
        zot = connect_to_zotero(args.library_id, args.library_type, args.api_key)
        print_progress("Connection established successfully", args.verbose)
        
        # List collections or items
        if args.list_collections:
            handle_collection_listing(zot, args.output_format, args.output_file, args.verbose)
        else:
            handle_item_listing(zot, args.collection, args.item_type, 
                               args.output_format, args.output_file, args.verbose,
                               args.google_username, args.google_app_password)
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()