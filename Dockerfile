FROM europe-north1-docker.pkg.dev/sondreskarsten-d7d14/r-images/r-base:latest

WORKDIR /app

COPY _targets.R .
COPY run.R .
COPY R/ R/

CMD ["Rscript", "run.R"]
