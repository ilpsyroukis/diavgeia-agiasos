import requests
import io
import os
try:
    import pypdf
except ImportError:
    os.system("pip install pypdf")
    import pypdf

def test_pdf():
    ada = "ΨΕΡΕ469ΗΣ0-ΧΒΖ"
    url = f"https://diavgeia.gov.gr/doc/{ada}"
    print(f"Downloading {url}")
    resp = requests.get(url)
    
    if resp.status_code == 200:
        pdf_file = io.BytesIO(resp.content)
        reader = pypdf.PdfReader(pdf_file)
        text = ""
        for page in reader.pages:
            t = page.extract_text()
            if t: text += t + "\n"
        print("Text extracted length:", len(text))
        print(text[:500])
    else:
        print("Failed to download", resp.status_code)

if __name__ == "__main__":
    test_pdf()
