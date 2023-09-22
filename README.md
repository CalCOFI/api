# calcofi-api
Test application programming interface (API)


## Run the API

Run the API in the background from server's RStudio Terminal:

```bash
sudo Rscript /share/github/api/run-api.R &
```

### stop

```bash
ps -eaf | grep run-api
# bebest     48394   43484  0 Aug17 pts/1    00:09:24 /usr/local/lib/R/bin/exec/R --no-save --no-restore --no-echo --no-restore --file=/share/github/api/run-api.R
# bebest     65066   43484  0 19:57 pts/1    00:00:00 grep --color=auto run-api
kill -9 48394
# [1]+  Killed                  Rscript /share/github/api/run-api.R
```

## Host API web service

Reference for setup:
 - https://www.rplumber.io/articles/hosting.html#pm2-1

### Install `pm2` service

Install `pm2`:

```bash
sudo su -
sudo -u shiny ln -s /share/.calcofi_db_pass.txt /home/shiny/.calcofi_db_pass.txt
sudo apt-get update
sudo apt-get install nodejs npm
sudo npm install -g pm2
exit
sudo pm2 startup
```

Setup web service:

```bash
sudo -u shiny pm2 start --interpreter="Rscript" --image-name="run-api" '/share/github/api/run-api.R'
sudo -u shiny pm2 save
```

Now open http://api.calcofi.io to see it working.

Maintenance operations:

```bash
sudo -u shiny pm2 restart run-api
sudo -u shiny pm2 stop run-api
sudo -u shiny pm2 start run-api
sudo -u shiny pm2 logs run-api
sudo -u shiny pm2 logs run-api --lines 1000
sudo -u shiny pm2 list
```