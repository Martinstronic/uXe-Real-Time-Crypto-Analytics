// auth.js
const basicAuth = (req, res, next) => {
    const auth = { login: process.env.AUTH_USER || 'usuario', password: process.env.AUTH_PASS || 'senha' };

    // Header Authorization
    const b64auth = (req.headers.authorization || '').split(' ')[1] || '';
    const [user, pass] = Buffer.from(b64auth, 'base64').toString().split(':');

    if (user && pass && user === auth.login && pass === auth.password) {
        return next();
    }

    // Solicita autenticação
    res.set('WWW-Authenticate', 'Basic realm="Acesso restrito"');
    res.status(401).send('Autenticação necessária.');
};

module.exports = basicAuth;
