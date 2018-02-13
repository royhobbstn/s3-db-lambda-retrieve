const Parser = require('expr-eval').Parser;

const expression = ['B1 / C2'];


const parser = new Parser();
const expr = parser.parse(expression.join(""));

const fields_key = ['B1', 'C2'];

const data = [
    { "13099": 40000, "13100": 50000 },
    { "13099": 100, "13100": 50 }
];


const evaluated = {};


Object.keys(data[0]).forEach(geoid => {
    const obj = {};
    fields_key.forEach((field, i) => {
        obj[field] = data[i][geoid];
    });
    evaluated[geoid] = expr.evaluate(obj);

});

console.log(evaluated);
