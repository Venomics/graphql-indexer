#!/usr/bin/env node

// Simple node.js contract executor
const { Address, ProviderRpcClient, TvmException } = require('everscale-inpage-provider');
const { EverscaleStandaloneClient } = require('everscale-standalone-client/nodejs');

const bodyParser = require('body-parser')
const express = require('express');
const app = express();
app.use(bodyParser.json({ limit: '50mb' }));


const ever = new ProviderRpcClient({
    fallback: () =>
        EverscaleStandaloneClient.create({
            connection: {
                id: 2, // network id
                type: 'graphql',
                data: {
                    endpoints: ['https://gql-testnet.venom.foundation/graphql'],
                },
            },
        }),
});

async function execute(req) {
    console.log("Executing %s on %s", req.method, req.address)
    await ever.ensureInitialized();

    await ever.requestPermissions({
        permissions: ['basic'],
    });

    const address = new Address(req.address);

    const contract = new ever.Contract(req.abi, address);
    // arguments are hard-coded for now
    const output = await contract.methods[req.method]({ "answerId": 1 }).call();
    let res = {}
    if (output.left) {
        res.output = [
            output.left._address,
            output.right._address,
            output.lp._address,
        ]
    } else if (output.roots) {
        res.output = [
            output.roots[0],
            output.roots[1],
            output.lp._address,
        ]
    }

    return res;
}

app.post('/execute', async (req, res, next) => {
    try {
        const result = await execute(req.body);
        res.send(result);
    } catch (err) {
        next(err);
    }
})

const server = app.listen(9090, function () {
    const host = server.address().address
    const port = server.address().port

    console.log("Listening at http://%s:%s", host, port)
})

