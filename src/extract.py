import json
from bs4 import BeautifulSoup


def extract(file, save):
    # Load the HTML file
    with open(file, 'r') as f:
        html = f.read()
        # Parse the HTML and extract the conversation data
        soup = BeautifulSoup(html, 'html.parser')
        user_divs = soup.find_all('div', {'class': 'min-h-[20px] flex flex-col items-start gap-4 whitespace-pre-wrap'})
        data = [div.get_text() for div in user_divs]
        d = [
            json.dumps({'user': i, 'chatGPT': k})
            for i, k in zip(data[0::2], data[1::2])
        ]
    # Create a new PDF file and write the conversation data to it
    with open(save, 'w') as f:
        for dd in d:
            f.write("%s\n" % dd)


def main():
    html_file = '/home/shubham/Desktop/Job_Attire_Advice.html'
    save = '/home/shubham/Desktop/Job_Attire_Advice.jsonl'
    extract(file=html_file, save=save)


if __name__ == '__main__':
    main()
