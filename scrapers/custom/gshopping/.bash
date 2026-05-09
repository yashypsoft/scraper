python3.10 -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install \
  selenium \
  undetected-chromedriver \
  beautifulsoup4 \
  requests \
  lxml \
  pydub \
  pandas \
  fake-useragent \
  python-dateutil \
  SpeechRecognition

python gshopping/gscrapper.py


python gshopping/gscrapperci.py --chunk-id 1 --total-chunks 1 --input-file "google_shopping1.csv"