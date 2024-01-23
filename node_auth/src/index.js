const puppeteer = require('puppeteer');
const cache = require('memory-cache');
const fs = require('fs');
const crypto = require('crypto');

// Require the framework and instantiate it
const fastify = require('fastify')({logger: true})

async function saveLocalStorageToCache(page, cacheKey) {
    const localStorage = await page.evaluate(() => Object.assign({}, window.localStorage));
    cache.put(cacheKey, JSON.stringify(localStorage), 86_400_000, function (key, value) {
        console.log(`${key} cache timeout`);
    });
}

async function saveLocalStorageToFile(page) {
    const localStorage = await page.evaluate(() => Object.assign({}, window.localStorage));
    fs.writeFileSync('localStorage.json', JSON.stringify(localStorage));
}

async function loadLocalStorageFromCache(page, cacheKey) {
    const cacheData = cache.get(cacheKey);
    if (cacheData) {
        const values = JSON.parse(cacheData);
        await page.evaluate(values => {
            Object.keys(values).forEach(key => {
                window.localStorage.setItem(key, values[key]);
            });
        }, values);

        return true;
    }

    return false;
}

async function loadLocalStorageFromFile(page) {
    if (!fs.existsSync('localStorage.json')) {
        return false;
    }

    const values = JSON.parse(fs.readFileSync('localStorage.json', 'utf8'));
    await page.evaluate(values => {
        Object.keys(values).forEach(key => {
            localStorage.setItem(key, values[key]);
        });
    }, values);

    return true;
}

async function hashPassword(password) {
    const crypto = require('crypto');
    // const salt = crypto.randomBytes(16).toString('hex');
    const salt = "b1f9c2a5d8c0c2c4f4f2c7c0f0f7c1f1";
    const hash = crypto.pbkdf2Sync(password, salt, 1000, 64, 'sha512').toString('hex');
    return hash;
}

async function getAccessToken(page) {
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

    return accessToken;
}

function isTokenExpired(token) {
    if (!token) {
        return true;
    }
    const decodedToken = JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString('utf8'));
    const expirationDate = new Date(decodedToken.exp * 1000);
    const now = new Date();
    // If the token expires in less than 5 minutes, consider it expired
    return addMinutes(now, 5) > expirationDate;
}

function addMinutes(date, minutes) {
    return new Date(date.getTime() + minutes * 60000);
}

// Declare a route
fastify.post('/wattivahti/token', async (request, reply) => {
    const username = request.body.username;
    const password = request.body.password;
    const hashedPassword = await hashPassword(password);
    const skipCache = request.body.skipCache;

    if (!username || !password) {
        reply
            .code(400)
            .header('Content-Type', 'application/json; charset=utf-8')
            .send({status: 'error', message: 'username and password are required'});
        return;
    }

    const localStorageCacheKey = `${username}_${hashedPassword}_localStorage`;
    const cachedToken = cache.get(`${username}_${hashedPassword}_accessToken`);
    if (cachedToken && !skipCache && !isTokenExpired(cachedToken)) {
        return {status: "ok", accessToken: cachedToken}
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

    let accessToken = "";

    // Attempt loading the access token from the cache or file
    if ((await loadLocalStorageFromCache(page, localStorageCacheKey)) || (await loadLocalStorageFromFile(page))) {
        await page.goto('https://www.wattivahti.fi/wattivahti/metering', {
            waitUntil: 'networkidle2',
        });

        const [button] = await page.$x("//button[contains(., 'Kirjaudu sisään')]");
        if (!button) {
            // Wait for the Graph to be rendered
            accessToken = await page.waitForSelector('span[id=recharts_measurement_span]', {timeout: 30000})
                .then(async result => {
                    return await getAccessToken(page);
                })
                .catch(ex => {
                    console.error(ex);
                    return "";
                });
        }
    }

    // If the access token wasn't found, login
    if (!accessToken || accessToken === "") {
        // Go to the login page
        const [button] = await page.$x("//button[contains(., 'Kirjaudu sisään')]");
        if (!button) {
            reply
                .code(400)
                .header('Content-Type', 'application/json; charset=utf-8')
                .send({status: 'error', message: 'Login failed because login button couldn\'t be found.'});
            return;
        }
        await Promise.all([
            await button.click(),
            page.waitForNavigation({waitUntil: 'networkidle2'})
        ]);

        // Fill in the credentials
        await page.waitForSelector('input[id="logonIdentifier"]', {timeout: 5000});
        await page.type('#logonIdentifier', username, {delay: 100});
        await page.type('#password', password, {delay: 100});

        // Submit login
        await page.click('button[id="next"]');

        // Wait for the Graph to be rendered
        await page.waitForSelector('span[id=recharts_measurement_span]', {timeout: 30000});

        // Fetch the access token
        accessToken = await getAccessToken(page);
    }

    saveLocalStorageToCache(page, localStorageCacheKey);
    // await page.screenshot({ path: 'example.png' });

    await browser.close();

    if (!accessToken || accessToken === "") {
        reply
            .code(400)
            .header('Content-Type', 'application/json; charset=utf-8')
            .send({status: 'error', message: 'Fetching the access token failed.'});
        return;
    }

    cache.put(`${username}_${hashedPassword}_accessToken`, accessToken, 1_800_000, function (key, value) {
        console.log(`${key} cache timeout`);
    });

    return {status: "ok", accessToken: accessToken}
});

// Run the server!
const start = async () => {
    try {
        await fastify.listen({port: 3000, host: '0.0.0.0'})
    } catch (err) {
        fastify.log.error(err)
        process.exit(1)
    }
}

start()
