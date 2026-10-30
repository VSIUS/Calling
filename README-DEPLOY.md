# Call signaling server V7

## Files
- `server.js`: Socket.IO signaling server.
- `package.json`: Node.js dependencies and start command.

## Deploy to the existing Render service
1. In the `VSIUS/Calling` GitHub repository, replace the existing `server.js` and `package.json` with these two files (the repository currently has them at its root).
2. Commit and push to the `main` branch.
3. In Render, open the existing Web Service `calling-prb4`.
4. Keep its current Root Directory setting if the service currently runs from the repository root. Set Build Command to `npm install` and Start Command to `npm start` if those are not already configured.
5. Deploy the latest commit.
6. Check `https://calling-prb4.onrender.com/health`. Expected JSON includes `ok: true` and `protocol: "v7"`.

Do not change the Root Directory to `server` for this repository layout: the current GitHub repository has `server.js` and `package.json` in its root.

## Protocol
- `create-room` creates a random six-digit room and returns `room-joined` with role `host` and ICE server configuration.
- `join-room` accepts `{ "room": "123456" }`, returns `room-joined` with role `guest`, and notifies the host with `peer-joined`.
- `signal` relays WebRTC `offer`, `answer`, and `candidate` data to the other participant.
- `leave-room` and disconnect clean up room membership.

## TURN
STUN does not guarantee connectivity through every NAT/firewall. Optional TURN can be configured in Render Environment using `TURN_URLS`, `TURN_USERNAME`, and `TURN_CREDENTIAL`. Use real credentials from a TURN provider; do not copy placeholder values.

This server handles signaling, not audio media. Actual voice calling still depends on the Android WebRTC implementation and may require a working TURN service. Test with two devices on different networks after deployment.
