# calcofi-api
Application programming interface (API) for CalCOFI.io.

## Restart the Production API

Since adding the API as its own dedicated Docker container ([+ plumber api container, ssd disk · CalCOFI/server@727e708](https://github.com/CalCOFI/server/commit/727e7087f13083ecfa905c33dbd0a18315177942) on Nov 15, 2024), to restart the service (by having the container restart `/share/github/api/`[`run-api.R`](https://github.com/CalCOFI/api/blob/b02eccdf262cf9f525ee76a8c03d62efa2573955/run-api.R)), you simply need to login to the terminal of the server and restart the Docker container:

```bash
# change directory to the Github server repo with Docker compose container configuration files
cd /share/github/server

# restart the plumber container
docker compose restart plumber

# inspect logs to check for any problems
docker logs plumber
```

## Develop the API

Within this API repository, open the `plumber.R` file in RStudio and click on the **Run API** button.

![](https://rstudio.github.io/cheatsheets/html/images/plumber-ide.png)

For more, see [REST APIs with plumber :: Cheatsheet](https://rstudio.github.io/cheatsheets/html/plumber.html#running-plumber-apis)

## Git pane missing?

If so, in Terminal run `git status` to provide command you need to run:

```bash
git config --global --add safe.directory /share/github/api
```

Then open a different RStudio project (upper right), and this one again to return the Git pane in RStudio.
