use bbq2::traits::coordination::cs::CsCoord;
use clap::Parser;
use embedded_io_async::ErrorKind;
use ergot::exports::mutex::raw_impls::cs::CriticalSectionRawMutex;
use ergot::interface_manager::interface_impls::embedded_io::{IoInterface, SerialSink};
use ergot::interface_manager::profiles::direct_edge::DirectEdge;
use ergot::interface_manager::profiles::direct_edge::eio_0_6::RxWorker;
use ergot::{Address, NetStack};
use ergot::interface_manager::InterfaceState;
use ergot::toolkits::embedded_io_async_v0_6::tx_worker;
use ergot::well_known::ErgotPingEndpoint;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::join;
use tokio_serial::SerialPortBuilderExt;

use critical_section as _;

#[derive(clap::Parser, Debug)]
struct Args {
    #[clap(short, long)]
    serial: String,
}

type Queue = ergot::interface_manager::interface_impls::embedded_io::Queue<4096, CsCoord>;
type Interface = IoInterface<&'static Queue>;
type Profile = DirectEdge<Interface>;
static TX_QUEUE: Queue = Queue::new();
static STACK: NetStack<CriticalSectionRawMutex, Profile> = NetStack::new_with_profile(
    Profile::new_controller(SerialSink::new(TX_QUEUE.stream_producer(), 256), InterfaceState::Active {
        node_id: 1,
        net_id: 1,
    }),
);

struct Rx(tokio::io::ReadHalf<tokio_serial::SerialStream>);
impl embedded_io_async::ErrorType for Rx {
    type Error = ErrorKind;
}

impl embedded_io_async::Read for Rx {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.0.read(buf).await.map_err(|_e| ErrorKind::BrokenPipe)
    }
}

struct Tx(tokio::io::WriteHalf<tokio_serial::SerialStream>);

impl embedded_io_async::ErrorType for Tx {
    type Error = ErrorKind;
}

impl embedded_io_async::Write for Tx {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        self.0.write(buf).await.map_err(|_e| ErrorKind::BrokenPipe)
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    env_logger::init();

    let serial = tokio_serial::new(args.serial, 115200).open_native_async().unwrap();
    let (rx, tx) = tokio::io::split(serial);

    let mut rx = RxWorker::new(&STACK, Rx(rx), ());
    rx.set_controller(true);
    let mut frame_rx_buffer = [0u8; 256];
    let mut frame_scratch = [0u8; 256];
    /*tokio::spawn(ping_handler());
    tokio::spawn(pinger());*/
    let mut tx = Tx(tx);
    let _ = join!(
        tx_worker(&mut tx, TX_QUEUE.stream_consumer()),
        rx.run(&mut frame_rx_buffer, &mut frame_scratch),
        ping_handler(),
        pinger());
}

async fn ping_handler() {
    STACK.services().ping_handler::<2>().await;
}

async fn pinger() {
    let mut cnt = 0;
    let client = STACK.endpoints().client::<ErgotPingEndpoint>(Address {
        network_id: 1,
        node_id: 2,
        port_id: 0
    }, None);
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        match tokio::time::timeout(tokio::time::Duration::from_millis(250), client.request(&cnt)).await {
            Ok(Ok(n)) => {
                println!("Ping response: {}/{}", cnt, n);
            },
            Ok(Err(e)) => {
                println!("Ping net stack error {:?}", e);
            },
            Err(_) => {
                println!("Ping timeout");
            }
        }
        cnt = cnt.wrapping_add(1);
    }
}
