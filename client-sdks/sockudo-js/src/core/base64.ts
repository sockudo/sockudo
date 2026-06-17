export default function encode(s: any): string {
  return btoa(utob(s));
}

const fromCharCode = String.fromCharCode;

const b64chars =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const b64tab = {};

for (let i = 0, l = b64chars.length; i < l; i++) {
  b64tab[b64chars.charAt(i)] = i;
}

const cb_utob = function (c) {
  const cc = c.charCodeAt(0);
  return cc < 0x80
    ? c
    : cc < 0x800
      ? fromCharCode(0xc0 | (cc >>> 6)) + fromCharCode(0x80 | (cc & 0x3f))
      : fromCharCode(0xe0 | ((cc >>> 12) & 0x0f)) +
        fromCharCode(0x80 | ((cc >>> 6) & 0x3f)) +
        fromCharCode(0x80 | (cc & 0x3f));
};

const utob = function (u) {
  return u.replace(/[\u0080-\uFFFF]/g, cb_utob);
};

const cb_encode = function (ccc) {
  const padlen = [0, 2, 1][ccc.length % 3];
  const ord =
    (ccc.charCodeAt(0) << 16) |
    ((ccc.length > 1 ? ccc.charCodeAt(1) : 0) << 8) |
    (ccc.length > 2 ? ccc.charCodeAt(2) : 0);
  const chars = [
    b64chars.charAt(ord >>> 18),
    b64chars.charAt((ord >>> 12) & 63),
    padlen >= 2 ? "=" : b64chars.charAt((ord >>> 6) & 63),
    padlen >= 1 ? "=" : b64chars.charAt(ord & 63),
  ];
  return chars.join("");
};

const btoa =
  global.btoa ||
  function (b) {
    return b.replace(/[\s\S]{1,3}/g, cb_encode);
  };
