# calcofi-api
Test application programming interface (API)


## Run the API

Run the API in the background from server's RStudio Terminal:

```bash
Rscript /share/github/calcofi-api/run_api.R &
```

## Host API web service

Reference for setup:
 - https://www.rplumber.io/articles/hosting.html#pm2-1

### Install `pm2` service

Install `pm2`:

```bash
sudo su -
sudo -u shiny ln -s /share/.calcofi_db_pass.txt /home/shiny/.calcofi_db_pass.txt
apt-get update
apt-get install nodejs npm
npm install -g pm2
exit
sudo pm2 startup
```

Setup web service:

```bash
sudo -u shiny pm2 start --interpreter="Rscript" --image-name="run-api" /share/github/api/run-api.R
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