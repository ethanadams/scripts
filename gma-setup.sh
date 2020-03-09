sudo apt update
sudo apt install -y git
cd ~
wget https://dl.google.com/go/go1.13.8.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.13.8.linux-amd64.tar.gz
echo -e "\nexport PATH=$PATH:/usr/local/go/bin:~/go/bin\n" >> ~/.profile
source ~/.profile
echo -e "Host github.com\n\tStrictHostKeyChecking no\n" >> ~/.ssh/config
git clone https://github.com/ethanadams/scripts.git
cd ~/scripts/downloader && go install ./... && cd ~
