import { Server } from "socket.io";
import { Redis } from "ioredis";



const pub = new Redis({
  host: 'redis-3037cbb-dipsundarjana-a109.c.aivencloud.com',
  port: 14954,
  username: 'default',
  password: 'AVNS_PxIFHhJ8fuFeLZSn7l4'
})

const sub = new Redis({
  host: 'redis-3037cbb-dipsundarjana-a109.c.aivencloud.com',
  port: 14954,
  username: 'default',
  password: 'AVNS_PxIFHhJ8fuFeLZSn7l4'
})

class SocketService {
  private _io: Server;

  constructor() {
    console.log("Init Socket Service...");
    this._io = new Server({
      cors: {
        allowedHeaders: ["*"],
        origin: "*",
      },
    });
    sub.subscribe("MESSAGES");
  }


  public initListeners() {
    const io = this.io;
    console.log("Init Socket Listeners....")
    io.on("connect", (socket) => {
      console.log(`New Socket Connected`, socket.id);

      socket.on("event:message", async ({ message }: { message: string }) => {
        console.log("New Message Res.", message);
        // publish this message to redis
        await pub.publish("MESSAGES", JSON.stringify({message}))

      });
    });

    sub.on("message", (channel, message) => {
      if (channel === "MESSAGES") {
        io.emit("message", message);
      }
    })
  }

  get io() {
    return this._io;
  }
}

export default SocketService;
