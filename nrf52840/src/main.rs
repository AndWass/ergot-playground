#![no_std]
#![no_main]

use bbq2::traits::coordination::cs::CsCoord;
use embassy_executor::Spawner;
use embassy_futures::join::join;
use embassy_nrf::bind_interrupts;
use embassy_time::{Duration, Ticker, Timer, WithTimeout};
use ergot::{Address, NetStack};
use ergot::exports::mutex::raw_impls::cs::CriticalSectionRawMutex;
use ergot::interface_manager::interface_impls::embedded_io::{IoInterface, SerialSink};
use ergot::interface_manager::profiles::direct_edge::DirectEdge;
use ergot::interface_manager::profiles::direct_edge::eio_0_6::RxWorker;
use ergot::toolkits::embedded_io_async_v0_6::tx_worker;
use ergot::well_known::ErgotPingEndpoint;
use {defmt_rtt as _, panic_probe as _};

bind_interrupts!(
    /// Binds the SPIM3 interrupt.
    struct Irqs {
        UARTE0 => embassy_nrf::buffered_uarte::InterruptHandler<embassy_nrf::peripherals::UARTE0>;
    }
);

type Queue = ergot::interface_manager::interface_impls::embedded_io::Queue<4096, CsCoord>;
type Interface = IoInterface<&'static Queue>;
type Profile = DirectEdge<Interface>;
static TX_QUEUE: Queue = Queue::new();
static STACK: NetStack<CriticalSectionRawMutex, Profile> = NetStack::new_with_profile(
    Profile::new_target(SerialSink::new(TX_QUEUE.stream_producer(), 256)),
);

#[embassy_executor::main]
async fn main(spawner: Spawner) {
    let p = embassy_nrf::init(Default::default());
    let mut rx_buffer = [0u8; 256];
    let mut tx_buffer = [0u8; 256];
    let (rx, mut tx) = embassy_nrf::buffered_uarte::BufferedUarte::new(
        p.UARTE0,
        p.TIMER0,
        p.PPI_CH0,
        p.PPI_CH1,
        p.PPI_GROUP0,
        p.P0_08,
        p.P0_06,
        Irqs,
        Default::default(),
        &mut rx_buffer,
        &mut tx_buffer,
    )
    .split();
    let mut rx = RxWorker::new(&STACK, rx, ());
    let mut frame_rx_buffer = [0u8; 256];
    let mut frame_scratch = [0u8; 256];
    spawner.spawn(ping_handler()).unwrap();
    spawner.spawn(pinger()).unwrap();
    let _ = join(
        tx_worker(&mut tx, TX_QUEUE.stream_consumer()),
        rx.run(&mut frame_rx_buffer, &mut frame_scratch),).await;
}

#[embassy_executor::task]
async fn ping_handler() {
    STACK.services().ping_handler::<2>().await;
}

#[embassy_executor::task]
async fn pinger() {
    let mut ticker = Ticker::every(Duration::from_secs(1));
    let mut cnt = 0;
    loop {
        ticker.next().await;
        match STACK.endpoints().request::<ErgotPingEndpoint>(Address {
            network_id: 1,
            node_id: 1,
            port_id: 0
        }, &cnt, None).with_timeout(Duration::from_millis(500)).await {
            Ok(Ok(n)) => {
                defmt::info!("Ping response: {}/{}", cnt, n);
            },
            Ok(Err(_)) => {
                defmt::warn!("Ping net stack error");
            },
            Err(_) => {
                defmt::warn!("Ping timeout");
            }
        }
        cnt = cnt.wrapping_add(1);
    }
}
