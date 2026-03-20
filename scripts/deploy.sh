#!/bin/bash
MASTER="ubuntu@130.238.27.15"
echo "Deploying to $MASTER..."
scp -r src/ $MASTER:~/src/
scp -r scripts/ $MASTER:~/scripts/
echo "Deploy complete!"
