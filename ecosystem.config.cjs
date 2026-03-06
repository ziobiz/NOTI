/**
 * PM2 설정 (선택)
 * 사용: pm2 start ecosystem.config.cjs
 */
module.exports = {
  apps: [
    {
      name: 'pg-noti-relay',
      script: 'server.js',
      instances: 1,
      exec_mode: 'fork',
      env: {
        NODE_ENV: 'development',
      },
      env_production: {
        NODE_ENV: 'production',
      },
      max_memory_restart: '300M',
    },
  ],
};
