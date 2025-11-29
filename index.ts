import { BskyAgent } from '@atproto/api';
import * as dotenv from 'dotenv';
import * as process from 'process';

dotenv.config();

// Constants
const MARKETPLACE_ADMIN_DID = 'did:plc:v576dt52yejwpijgyx7b6cwh';
const DM_SERVICE_HEADERS = { "atproto-proxy": "did:web:api.bsky.chat" };
const CHECK_INTERVAL_MS = 5000; // Check messages every 5 seconds
const LIST_REFRESH_INTERVAL_MS = 5 * 60 * 1000; // Refresh lists every 5 minutes
const DISPATCH_INTERVAL_MS = 30 * 1000; // Notify next driver every 30 seconds

// Types
interface DriverState {
    status: 'AVAILABLE' | 'UNAVAILABLE';
    location?: { latitude: number; longitude: number };
    lastSeen: number;
    type: 'DRIVERS' | 'LIVREURS';
    did: string;
}

interface Candidate {
    did: string;
    distance: number;
}

interface ActiveRequest {
    id: string;
    type: 'RIDE' | 'DELIVERY';
    status: 'PENDING' | 'ASSIGNED';
    customerDid: string;
    candidates: Candidate[];
    nextCandidateIndex: number;
    notifiedDrivers: string[];
    data: any;
    timestamp: number;
    lastNotificationTime: number;
}

// State
const drivers = new Map<string, DriverState>();
const activeRequests = new Map<string, ActiveRequest>();
const validDrivers = new Set<string>();
const validLivreurs = new Set<string>();

// Agent
const agent = new BskyAgent({
    service: 'https://bsky.social',
});

// --- Helpers ---

function getDistance(lat1: number, lon1: number, lat2: number, lon2: number) {
    const R = 6371e3; // metres
    const φ1 = lat1 * Math.PI / 180;
    const φ2 = lat2 * Math.PI / 180;
    const Δφ = (lat2 - lat1) * Math.PI / 180;
    const Δλ = (lon2 - lon1) * Math.PI / 180;

    const a = Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
        Math.cos(φ1) * Math.cos(φ2) *
        Math.sin(Δλ / 2) * Math.sin(Δλ / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    return R * c;
}

async function refreshDriverLists() {
    console.log('Refreshing driver lists...');
    try {
        const res = await agent.app.bsky.graph.getLists({
            actor: MARKETPLACE_ADMIN_DID,
            limit: 50,
        });

        if (!res.success) return;

        const driversList = res.data.lists.find(l => l.description?.includes('[MODULE]=[DRIVERS]'));
        const livreursList = res.data.lists.find(l => l.description?.includes('[MODULE]=[LIVREURS]'));

        if (driversList) {
            const items = await agent.app.bsky.graph.getList({ list: driversList.uri, limit: 100 });
            validDrivers.clear();
            items.data.items.forEach(i => validDrivers.add(i.subject.did));
            console.log(`Loaded ${validDrivers.size} drivers`);
        }

        if (livreursList) {
            const items = await agent.app.bsky.graph.getList({ list: livreursList.uri, limit: 100 });
            validLivreurs.clear();
            items.data.items.forEach(i => validLivreurs.add(i.subject.did));
            console.log(`Loaded ${validLivreurs.size} livreurs`);
        }
    } catch (e) {
        console.error('Error refreshing lists:', e);
    }
}

async function sendDM(did: string, message: any) {
    try {
        const convo = await agent.api.chat.bsky.convo.getConvoForMembers(
            { members: [did] },
            { headers: DM_SERVICE_HEADERS }
        );
        
        await agent.api.chat.bsky.convo.sendMessage(
            {
                convoId: convo.data.convo.id,
                message: { text: typeof message === 'string' ? message : JSON.stringify(message) },
            },
            { encoding: 'application/json', headers: DM_SERVICE_HEADERS }
        );
        return true;
    } catch (e) {
        console.error(`Failed to send DM to ${did}:`, e);
        return false;
    }
}

// --- Handlers ---

async function handleDriverAvailability(senderDid: string, payload: any) {
    // Verify sender is a driver/livreur
    let type: 'DRIVERS' | 'LIVREURS' | null = null;
    if (validDrivers.has(senderDid)) type = 'DRIVERS';
    else if (validLivreurs.has(senderDid)) type = 'LIVREURS';

    if (!type) {
        console.log(`Ignored availability from non-driver ${senderDid}`);
        return;
    }

    drivers.set(senderDid, {
        did: senderDid,
        status: payload.status,
        location: payload.location,
        lastSeen: Date.now(),
        type
    });
    console.log(`Updated driver ${senderDid}: ${payload.status}`);
}

async function handleRequest(senderDid: string, payload: any) {
    const isDelivery = payload.type === 'DELIVERY_REQUEST';
    const reqType = isDelivery ? 'LIVREURS' : 'DRIVERS';
    const reqId = payload.orderId;

    console.log(`New ${payload.type} from ${senderDid}: ${reqId}`);

    // Find nearest drivers
    // Handle both old format (deliveryLocation) and new format (pickup)
    const reqLat = payload.pickup?.latitude || payload.deliveryLocation?.latitude;
    const reqLon = payload.pickup?.longitude || payload.deliveryLocation?.longitude;

    if (!reqLat || !reqLon) {
        console.error('Request missing location');
        return;
    }

    const candidates = Array.from(drivers.values())
        .filter(d => d.type === reqType && d.status === 'AVAILABLE' && d.location)
        .map(d => ({
            did: d.did,
            distance: getDistance(reqLat, reqLon, d.location!.latitude, d.location!.longitude)
        }))
        .sort((a, b) => a.distance - b.distance);

    if (candidates.length === 0) {
        console.log('No drivers available');
        await sendDM(senderDid, { type: 'NO_DRIVERS_AVAILABLE', orderId: reqId });
        return;
    }

    // Store request
    activeRequests.set(reqId, {
        id: reqId,
        type: isDelivery ? 'DELIVERY' : 'RIDE',
        status: 'PENDING',
        customerDid: senderDid,
        candidates: candidates,
        nextCandidateIndex: 0,
        notifiedDrivers: [],
        data: payload,
        timestamp: Date.now(),
        lastNotificationTime: 0 // Will trigger immediate dispatch in processRequests
    });
    
    console.log(`Request ${reqId} queued with ${candidates.length} candidates`);
}

async function processRequests() {
    const now = Date.now();
    
    for (const [reqId, req] of activeRequests.entries()) {
        if (req.status !== 'PENDING') continue;

        // Check if we need to notify the next driver
        if (now - req.lastNotificationTime >= DISPATCH_INTERVAL_MS) {
            if (req.nextCandidateIndex < req.candidates.length) {
                const candidate = req.candidates[req.nextCandidateIndex];
                
                // Check if driver is still available (optional, but good practice)
                const driverState = drivers.get(candidate.did);
                if (driverState && driverState.status === 'AVAILABLE') {
                    console.log(`Offering request ${reqId} to driver ${candidate.did} (${Math.round(candidate.distance)}m away)`);
                    
                    const requestMsg = {
                        ...req.data,
                        type: req.type === 'DELIVERY' ? 'DELIVERY_OFFER' : 'RIDE_OFFER',
                    };

                    await sendDM(candidate.did, requestMsg);
                    req.notifiedDrivers.push(candidate.did);
                    req.lastNotificationTime = now;
                    req.nextCandidateIndex++;
                } else {
                    // Driver went offline or unavailable, skip
                    console.log(`Skipping driver ${candidate.did} (unavailable)`);
                    req.nextCandidateIndex++;
                    // We'll try the next one in the next loop iteration (or immediately if we used a while loop, but let's keep it simple)
                }
            } else {
                // No more candidates
                if (req.notifiedDrivers.length > 0 && now - req.lastNotificationTime > DISPATCH_INTERVAL_MS * 2) {
                    // If we've notified everyone and waited a while, maybe expire the request?
                    // For now, we just leave it pending until someone accepts or it's cancelled
                }
            }
        }
    }
}

async function handleAccept(senderDid: string, payload: any) {
    const reqId = payload.orderId;
    const req = activeRequests.get(reqId);

    if (!req) {
        console.log(`Driver ${senderDid} accepted unknown/expired order ${reqId}`);
        return;
    }

    if (req.status !== 'PENDING') {
        console.log(`Driver ${senderDid} accepted already assigned order ${reqId}`);
        await sendDM(senderDid, { type: 'ORDER_ALREADY_TAKEN', orderId: reqId });
        return;
    }

    // Assign
    req.status = 'ASSIGNED';
    console.log(`Order ${reqId} assigned to ${senderDid}`);

    // 1. Notify Customer
    const driverInfo = drivers.get(senderDid);
    await sendDM(req.customerDid, {
        type: 'ORDER_ASSIGNED',
        orderId: reqId,
        driver: {
            did: senderDid,
            location: driverInfo?.location
        }
    });

    // 2. Notify other drivers
    const others = req.notifiedDrivers.filter(d => d !== senderDid);
    for (const otherDid of others) {
        await sendDM(otherDid, {
            type: 'ORDER_TAKEN_BY_OTHER',
            orderId: reqId
        });
    }

    // 3. Confirm to driver
    await sendDM(senderDid, {
        type: 'ORDER_CONFIRMED',
        orderId: reqId,
        customerDid: req.customerDid,
        data: req.data
    });
}

// --- Main Loop ---

async function checkMessages() {
    try {
        const res = await agent.api.chat.bsky.convo.listConvos(
            { limit: 20 },
            { headers: DM_SERVICE_HEADERS }
        );

        for (const convo of res.data.convos) {
            if (convo.unreadCount > 0) {
                // Fetch messages
                const msgs = await agent.api.chat.bsky.convo.getMessages(
                    { convoId: convo.id, limit: convo.unreadCount },
                    { headers: DM_SERVICE_HEADERS }
                );

                for (const msg of msgs.data.messages) {
                    // @ts-ignore
                    if (msg.sender?.did === agent.session?.did) continue; // Skip own messages

                    // @ts-ignore
                    const text = msg.text;
                    try {
                        const payload = JSON.parse(text);
                        // @ts-ignore
                        const senderDid = msg.sender.did;

                        switch (payload.type) {
                            case 'DRIVER_AVAILABILITY':
                                await handleDriverAvailability(senderDid, payload);
                                break;
                            case 'RIDE_REQUEST':
                            case 'DELIVERY_REQUEST':
                                await handleRequest(senderDid, payload);
                                break;
                            case 'RIDE_ACCEPTED':
                            case 'DELIVERY_ACCEPTED':
                                await handleAccept(senderDid, payload);
                                break;
                        }
                    } catch (e) {
                        // Not JSON, ignore
                    }
                }

                // Mark read
                await agent.api.chat.bsky.convo.updateRead(
                    { convoId: convo.id },
                    { encoding: 'application/json', headers: DM_SERVICE_HEADERS }
                );
            }
        }
    } catch (e) {
        console.error('Error checking messages:', e);
    }
}

async function main() {
    console.log('Starting Dispatcher Bot...');
    await agent.login({ identifier: process.env.BLUESKY_USERNAME!, password: process.env.BLUESKY_PASSWORD! });
    console.log('Logged in as:', agent.session?.did);

    // Initial list fetch
    await refreshDriverLists();

    // Schedule list refresh
    setInterval(refreshDriverLists, LIST_REFRESH_INTERVAL_MS);

    // Message polling loop
    setInterval(checkMessages, CHECK_INTERVAL_MS);
    
    // Request processing loop (gradual dispatch)
    setInterval(processRequests, 1000); // Check every second

    console.log('Bot is running...');
}

main().catch(console.error);