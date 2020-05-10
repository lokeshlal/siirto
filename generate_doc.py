import os
import shutil
import ntpath
from bs4 import BeautifulSoup


base_src_folder_path = os.path.abspath('./siirto')
doc_folder_path = os.path.abspath('./doc')
if os.path.exists(doc_folder_path):
    shutil.rmtree('./doc')
os.mkdir(doc_folder_path)


def generate_doc(base_src_folder, base_path, is_dir):
    """
    Generate the html doc for the entire project using pydoc3
    :param base_src_folder: path to module (or .py file)
    :param base_path: base path of module
    :param is_dir: is module a dir or file
    """
    name = ntpath.basename(base_src_folder)
    # skip __init__
    print(name)
    if name == "__init__" or name == "__pycache__":
        return
    if is_dir:
        current_module = f"{base_path}.{name}" if base_path else name
    else:
        current_module = f"{base_path}.{name.replace('.py', '')}" \
            if base_path else name.replace('.py', '')

    os.system(f"pydoc3 -w {current_module}")
    if is_dir:
        for path_object in os.listdir(base_src_folder):
            if os.path.isdir(os.path.join(base_src_folder, path_object)):
                generate_doc(os.path.join(base_src_folder, path_object), current_module, True)
            else:
                cm = current_module + "." + ntpath.basename(path_object).replace(".py", "")
                os.system(f"pydoc3 -w {cm}")


def parse_all_html():
    base_path = os.path.abspath("./")
    print(base_path)
    for html_file in os.listdir(base_path):
        if html_file.endswith(".html"):
            shutil.move(os.path.join(base_path, html_file), os.path.join(doc_folder_path, html_file))
            list_to_be_removed = []
            with open(os.path.join(doc_folder_path, html_file), "r") as file:
                file_content = file.read()
                soup = BeautifulSoup(file_content, features="html.parser")
                for links in soup.find_all("a"):
                    if links.get("href") \
                            and not links.get("href").startswith("siirto") \
                            and not links.get("href").startswith("file:"):
                        list_to_be_removed.append(f"href=\"{links.get('href')}\"")

            link_replace_string = "href=\"#\""
            for external_links in list_to_be_removed:
                file_content = file_content.replace(external_links, link_replace_string)
            with open(os.path.join(doc_folder_path, html_file), 'w') as file:
                file.write(file_content)


generate_doc(base_src_folder_path, None, True)
parse_all_html()
