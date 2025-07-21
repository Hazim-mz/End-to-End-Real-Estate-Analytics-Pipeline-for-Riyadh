FROM apache/airflow:3.0.0

USER root

# Install Chrome and ChromeDriver dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install ChromeDriver and other required packages using webdriver-manager approach
RUN pip install webdriver-manager selenium==4.15.2 beautifulsoup4==4.12.2 requests==2.31.0 lxml==4.9.3 pandas==2.1.4 numpy==1.26.2 python-dotenv==1.0.0 pyyaml==6.0.1 python-dateutil==2.8.2 pytz==2023.3 