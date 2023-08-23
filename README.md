# Calculate sleep quality metrics
Tool to extract sleep quality metrics using xgboost models.

## Install required R packages
```console
Rscript r_dependencies.R
```

## Calculate sleep quality metrics from .cwa files
```console
./get_sleep_stats.sh [path/to/files.cwa]
```

## Calculate sleep quality metrics from .wav files
```console
./get_sleep_stats.sh [path/to/files.wav]
```

You wll be prompted to enter either `cwa` or `wav` and the process will start.
