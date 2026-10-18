bash sig/tnrun.sh go wt-fix-1384-r73-dev --allow-dirty --ref neo-ref --parser both --env R73_REWRITES=1 -- ./pkg/server -run '^TestRule73PatternFamilyHTTPAttack$' -count=1 -timeout 30m -v

