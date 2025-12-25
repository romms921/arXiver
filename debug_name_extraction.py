
import re

def clean_latex_text(text):
    if not text: return ""
    text = re.sub(r'\\email\{[^}]*\}', '', text)
    text = re.sub(r'\\thanks\{[^}]*\}', '', text)
    text = re.sub(r'\\fnmsep', '', text)
    text = re.sub(r'\\vspace\{[^}]*\}', '', text)
    text = re.sub(r'\\corref\{[^}]*\}', '', text)
    
    for _ in range(4):
        new_text = re.sub(r'\\[a-zA-Z]+\{((?:[^{}]|\{[^{}]*\})*)\}', r'\1', text)
        if new_text == text: break
        text = new_text
    
    text = re.sub(r'\\[a-zA-Z]+', ' ', text)
    text = re.sub(r'[{}]', ' ', text)
    text = text.replace('\\\\', ', ')
    text = text.replace('\\', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^[\s,;.]+|[\s,;.]+$', '', text)
    return text

def extract_name_from_author(author_str):
    if not author_str: return ""
    print(f"Original: {author_str}")
    author_str = re.sub(r'\\altaffilmark\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\inst\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\orcidlink\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\footnote\{[^}]*\}', '', author_str)
    print(f"After specific removals: {author_str}")
    author_str = re.sub(r'\$[\^]*\{?[\w, \-$\star\dagger]*\}?\$', '', author_str)
    print(f"After superscript removal: {author_str}")
    name = clean_latex_text(author_str)
    return name.strip()

test_str = r"J.~Callow$^{\orcidlink{0000-0002-0804-9533}}$,$^{1}$\thanks{E-mail: joe.callow@port.ac.uk}"
print(f"Result: '{extract_name_from_author(test_str)}'")

print("-" * 20)
test_str_2 = r"O.~Graur$^{\orcidlink{0000-0002-4391-6137}}$,$^{1,2}$"
print(f"Result 2: '{extract_name_from_author(test_str_2)}'")
