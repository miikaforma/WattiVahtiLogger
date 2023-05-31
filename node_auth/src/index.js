const puppeteer = require('puppeteer');
const cache = require('memory-cache');

// Require the framework and instantiate it
const fastify = require('fastify')({ logger: true })

// Declare a route
fastify.post('/wattivahti/token', async (request, reply) => {
    const username = request.body.username;
    const password = request.body.password;
    const skipCache = request.body.skipCache;

    if (!username || !password) {
        reply
            .code(400)
            .header('Content-Type', 'application/json; charset=utf-8')
            .send({ status: 'error', message: 'username and password are required' });
        return;
    }

    const cachedToken = cache.get(`${username}_accessToken`);
    if (cachedToken && !skipCache) {
        return { status: "ok", accessToken: cachedToken }
    }

    const browser = await puppeteer.launch({
        pipe: true,
        headless: true,
        dumpio: true,
        args: [
            '--disable-dev-shm-usage',
            '--disable-setuid-sandbox',
            '--no-sandbox',
            '--no-zygote',
            '--disable-gpu',
            '--disable-audio-output',
            '--headless',
            '--single-process'
        ]
    });
    const page = await browser.newPage();
    await page.goto('https://www.wattivahti.fi/', {
        waitUntil: 'networkidle2',
    });

    // Go to the login page
    const [button] = await page.$x("//button[contains(., 'Kirjaudu sisään')]");
    if (!button) {
        reply
            .code(400)
            .header('Content-Type', 'application/json; charset=utf-8')
            .send({ status: 'error', message: 'Login failed because login button couldn\'t be found.' });
        return;
    }
    await Promise.all([
        await button.click(),
        page.waitForNavigation({ waitUntil: 'networkidle2' })
    ]);

    // Fill in the credentials
    await page.waitForSelector('input[id="logonIdentifier"]', { timeout: 5000 });
    await page.type('#logonIdentifier', username, { delay: 100 });
    await page.type('#password', password, { delay: 100 });

    // Submit login
    await page.click('button[id="next"]');

    // Wait for the Graph to be rendered
    await page.waitForSelector('span[id=recharts_measurement_span]', { timeout: 30000 });

    // Fetch the access token
    let accessToken = "";
    const localStorage = await page.evaluate(() => Object.assign({}, window.localStorage));
    for (const [key, value] of Object.entries(localStorage)) {
        if (key.includes("https://pesv.onmicrosoft.com/salpa/Customer.Read")) {
            const data = JSON.parse(value);
            if (data.accessToken) {
                accessToken = data.accessToken;
            }
        }
    }

    if (!accessToken || accessToken === "") {
        for (const [key, value] of Object.entries(localStorage)) {
            if (key.includes("-accesstoken-")) {
                const data = JSON.parse(value);
                if (data.secret) {
                    accessToken = data.secret;
                }
            }
        }
    }

    // await page.screenshot({ path: 'example.png' });

    await browser.close();

    if (!accessToken || accessToken === "") {
        reply
            .code(400)
            .header('Content-Type', 'application/json; charset=utf-8')
            .send({ status: 'error', message: 'Fetching the access token failed.' });
        return;
    }

    cache.put(`${username}_accessToken`, accessToken, 1_800_000, function(key, value) {
        console.log(`${key} cache timeout`);
    });

    return { status: "ok", accessToken: accessToken }
});

// Run the server!
const start = async () => {
    try {
        await fastify.listen({ port: 3000, host: '0.0.0.0' })
    } catch (err) {
        fastify.log.error(err)
        process.exit(1)
    }
}

start()
