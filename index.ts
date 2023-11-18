import fs from 'node:fs'
import fsp from 'node:fs/promises'
import path from 'node:path';
import { parser } from 'stream-json';
import { pick } from 'stream-json/filters/Pick';
import { Transform, TransformCallback } from 'stream';
import FilterBase from 'stream-json/filters/FilterBase';

const PATH = process.argv[2];
if (!PATH) {
    throw new Error("path not specified")
}

type Token = { readonly name: string; readonly value: TokenValue; }
type TokenValue = string | null | boolean | readonly Token[];

function isArray(value: TokenValue):  value is readonly Token[] {
    return Array.isArray(value);
}

class PGCollector extends Transform {
    constructor() {
        super({ objectMode: true });
    }

    #keys: string[] = [];
    #curObj: Token[] | undefined;
    #objStack: Token[][] = [];

    _transform(chunk: FilterBase.Token, encoding: BufferEncoding, callback: TransformCallback): void {
        switch (chunk.name) {
            case 'keyValue': {
                const key = chunk.value as string;
                this.#keys.push(key.startsWith('pgsql.') ? key.substring(6) : key)
                break;
            }
            case "stringValue":
            case "numberValue":
            case "nullValue":
            case "trueValue":
            case "falseValue": {
                const key = this.#keys.pop();
                if (!key) { callback(Error("expected a key")); return; }
                if (chunk.value === undefined) { callback(Error("expected a value")); return; }
                if (!this.#curObj) { callback(Error("expected a current object")); return; }
                this.#curObj.push({name: key, value: chunk.value })
                break;
            }
            case "startObject":
                if (this.#curObj) {
                    this.#objStack.push(this.#curObj);
                }
                this.#curObj = []
                break;
            case "endObject": {
                const key = this.#keys.pop();
                const curObj = this.#curObj;
                if (!curObj) { callback(Error("expected a current object")); return; }

                if (!key) {
                    this.#curObj = undefined;
                    this.push(curObj);
                } else {
                    this.#curObj = this.#objStack.pop();
                    if (!this.#curObj) { callback(Error("expected an object on the stack")); return; }
                    this.#curObj.push({name: key, value: curObj})
                }
                break;
            }
            default:
                callback(new Error(`unexpected name ${chunk.name}`));
                return;
        }
        callback();
    }
}

const pipeline = fs.createReadStream(PATH)
    .pipe(parser({ streamValues: false }))
    .pipe(pick({
        filter: (stack, token) => {
            return token.name === "startObject" && stack.at(-1) === "pgsql";
        }
    }))
    .pipe(new PGCollector())

    ;

type PGToken = readonly [key: string, value: PGTokenValue]
type PGTokenValue = string | boolean | null | readonly PGToken[]
function mapToken(t: Token): PGToken {
    if (isArray(t.value)) {
        const value = t.value.map(mapToken);
        return [t.name, value]
    } else {
        return [t.name, t.value]
    }
}
    

interface PGRecord {
    type?: string;
    frontend?: boolean;
    length?: number;
    data: PGToken[];
}

function convertPgRecord(data: Token[]): PGRecord {
    const $type = data.find(t => t.name === 'type')?.value;
    const type = $type ? $type.toString() : undefined;
    const $length = data.find(t => t.name === 'length')?.value;
    const length = $length ? +$length : undefined;
    const $frontend = data.find(t => t.name === 'frontend')?.value;
    const frontend = $frontend ? !!(+$frontend) : undefined;

    const $data = data
        .filter(t => !["type", "length", "frontend"].includes(t.name))
        .map(mapToken);
    return { type, frontend, length, data : $data}    
}



const records = new Array<PGRecord>()
pipeline.on('data', (data: Token[]) => {
    records.push(convertPgRecord(data));
});
pipeline.on('end', () => {
    const { dir, ext, name } = path.parse(PATH);
    const newPath = path.format({ dir, ext, name: `${name}.pgsql`})
    fsp.writeFile(newPath, JSON.stringify(records, null, 4)).catch(console.error);
});

