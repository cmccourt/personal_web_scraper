import re

import requests


def get_page_text(url):
    response = requests.get(url)
    res_text = response.text
    page_text_list, page_words = [], []
    html_content = res_text[res_text.find("<body") + 5:res_text.find("</body>")]
    start_script_tag = html_content.find("<script")
    ignore = []
    while start_script_tag > -1:
        # Remove any content within a script tag
        end_script = html_content.find("</script>")
        # Adding 9 to get end of </script> tag
        text_script = html_content[start_script_tag:end_script + 9]
        html_content = html_content.replace(text_script, "")
        start_script_tag = html_content.find("<script")
    try:
        with open("../settings/ignore_list.txt", "r") as ign_words_file:
            # Create a list of ignore words for comparison
            # convert word to lowercase and remove any whitespace
            ignore = [word.lower().strip() for word in ign_words_file]
    except IOError:
        print("Could not find the file")
    finished = False
    while not finished:
        next_close_tag = html_content.find(">")
        next_open_tag = html_content.find("<")
        if next_open_tag > -1:
            content = " ".join(html_content[next_close_tag + 1:next_open_tag].strip().split())
            page_text_list.append(content)
            html_content = html_content[next_open_tag + 1:]
        else:
            finished = True
    for word in page_text_list:
        if word[0].isalnum() and word.lower() not in ignore:
            word = re.sub(r'[^\w\s]', '', word)
            if word not in page_words:
                page_words.append(word)
    return page_words
