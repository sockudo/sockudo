// Test delta compression with bundled libraries
const http = require('http');

// Test configuration
const config = {
    host: 'localhost',
    port: 3000,
    channel: 'test-channel'
};

// Create a large message (512KB)
function createLargeMessage(iteration) {
    const size = 512 * 1024; // 512KB
    const data = {
        iteration: iteration,
        timestamp: new Date().toISOString(),
        // Large payload - mostly the same content
        payload: 'X'.repeat(size - 100) + `_ITERATION_${iteration}`
    };
    return JSON.stringify(data);
}

// Send a batch of identical messages
async function sendBatchMessages() {
    console.log('Sending batch of 5 identical 512KB messages...');

    const messages = [];
    for (let i = 0; i < 5; i++) {
        messages.push({
            name: `large-event-${i}`,
            channel: config.channel,
            data: createLargeMessage(1) // Same content for all
        });
    }

    const options = {
        hostname: config.host,
        port: config.port,
        path: '/trigger-batch',
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        }
    };

    return new Promise((resolve, reject) => {
        const req = http.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                console.log(`Response: ${res.statusCode}`);
                if (res.statusCode === 200) {
                    console.log('Batch messages sent successfully');
                    resolve();
                } else {
                    console.log('Response body:', data);
                    reject(new Error(`Failed with status ${res.statusCode}`));
                }
            });
        });

        req.on('error', reject);
        req.write(JSON.stringify({ events: messages }));
        req.end();
    });
}

// Send sequential messages
async function sendSequentialMessages() {
    console.log('Sending 5 sequential identical 512KB messages...');

    for (let i = 0; i < 5; i++) {
        const options = {
            hostname: config.host,
            port: config.port,
            path: '/trigger',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        };

        await new Promise((resolve, reject) => {
            const req = http.request(options, (res) => {
                let data = '';
                res.on('data', chunk => data += chunk);
                res.on('end', () => {
                    if (res.statusCode === 200) {
                        console.log(`  Message ${i+1} sent successfully`);
                        resolve();
                    } else {
                        reject(new Error(`Failed with status ${res.statusCode}`));
                    }
                });
            });

            req.on('error', reject);
            req.write(JSON.stringify({
                channel: config.channel,
                event: `large-event-seq-${i}`,
                data: createLargeMessage(1) // Same content for all
            }));
            req.end();
        });

        // Small delay between messages
        await new Promise(resolve => setTimeout(resolve, 100));
    }
}

// Main test
async function runTest() {
    console.log('Delta Compression Test');
    console.log('======================');
    console.log('Make sure you have:');
    console.log('1. Sockudo running on port 6001');
    console.log('2. Test server running on port 3000');
    console.log('3. Browser open at http://localhost:3000');
    console.log('4. Delta compression enabled in the browser');
    console.log('');

    try {
        // Test sequential messages (should work)
        await sendSequentialMessages();
        console.log('✓ Sequential messages test completed\n');

        // Wait a bit
        await new Promise(resolve => setTimeout(resolve, 2000));

        // Test batch messages (known issue - will likely fail with delta)
        console.log('Testing batch messages (known limitation with delta compression)...');
        await sendBatchMessages();
        console.log('✓ Batch messages test completed\n');

        console.log('Test completed! Check the browser console for results.');
        console.log('Expected behavior:');
        console.log('- Sequential messages: Should show high compression ratio (~95-99%)');
        console.log('- Batch messages: May show "bad checksum" errors (known limitation)');
    } catch (error) {
        console.error('Test failed:', error.message);
        process.exit(1);
    }
}

// Run the test
runTest();
