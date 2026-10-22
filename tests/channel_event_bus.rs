//! Event-bus patterns name their messages without a topic string: a .NET
//! integration event is the type created (`new OrderStartedIntegrationEvent`),
//! a handler class subscribes to its generic argument, MassTransit and
//! NServiceBus send message objects, Celery tasks are their names, and
//! RabbitMQ clients name the routing key or queue. Every edge records the
//! name as written beside its lowercased key.

use lidx::indexer::csharp::CSharpExtractor;
use lidx::indexer::extract::{EdgeInput, LanguageExtractor};
use lidx::indexer::go::GoExtractor;
use lidx::indexer::python::PythonExtractor;

/// `(kind, source, key, name, framework)` per channel edge.
fn channel_edges(edges: Vec<EdgeInput>) -> Vec<(String, String, String, String, String)> {
    edges
        .into_iter()
        .filter(|e| e.kind == "CHANNEL_PUBLISH" || e.kind == "CHANNEL_SUBSCRIBE")
        .map(|e| {
            let d: serde_json::Value =
                serde_json::from_str(e.detail.as_deref().unwrap_or("{}")).unwrap();
            (
                e.kind,
                e.source_qualname.unwrap_or_default(),
                e.target_qualname.unwrap_or_default(),
                d["name"].as_str().unwrap_or("").to_string(),
                d["framework"].as_str().unwrap_or("").to_string(),
            )
        })
        .collect()
}

fn own(v: &[(&str, &str, &str, &str, &str)]) -> Vec<(String, String, String, String, String)> {
    v.iter()
        .map(|(a, b, c, d, e)| {
            (
                a.to_string(),
                b.to_string(),
                c.to_string(),
                d.to_string(),
                e.to_string(),
            )
        })
        .collect()
}

#[test]
fn dotnet_integration_events_are_published_where_created_and_handled_by_type() {
    let src = "namespace Shop;\n\
public record IntegrationEvent;\n\
public record OrderStartedIntegrationEvent(string User) : IntegrationEvent;\n\
public class Orders {\n\
  public async Task Start(IOrderingIntegrationEventService svc) {\n\
    var evt = new OrderStartedIntegrationEvent(\"u\");\n\
    await svc.AddAndSaveEventAsync(evt);\n\
    var plain = new IntegrationEvent();\n\
    var note = new OrderStartedDomainEvent();\n\
  }\n\
}\n\
public class OrderStartedHandler : IIntegrationEventHandler<OrderStartedIntegrationEvent> {\n\
  public Task Handle(OrderStartedIntegrationEvent e) => Task.CompletedTask;\n\
}\n\
public class Notify : INotificationHandler<OrderStartedDomainEvent> {\n\
  public Task Handle(OrderStartedDomainEvent e) => Task.CompletedTask;\n\
}\n";
    let mut x = CSharpExtractor::new().unwrap();
    assert_eq!(
        channel_edges(x.extract(src, "M").unwrap().edges),
        own(&[
            (
                "CHANNEL_PUBLISH",
                "Shop.Orders.Start",
                "channel://orderstartedintegrationevent",
                "OrderStartedIntegrationEvent",
                "event-bus"
            ),
            (
                "CHANNEL_SUBSCRIBE",
                "Shop.OrderStartedHandler.Handle",
                "channel://orderstartedintegrationevent",
                "OrderStartedIntegrationEvent",
                "event-bus"
            ),
        ])
    );
}

#[test]
fn masstransit_and_nservicebus_messages_are_their_types() {
    let src = "namespace Shop;\n\
public class Checkout {\n\
  public async Task Go(IPublishEndpoint publishEndpoint, IMessageSession messageSession, IMediator mediator, HttpClient http) {\n\
    await publishEndpoint.Publish(new OrderSubmitted());\n\
    await publishEndpoint.Publish<OrderAccepted>(new { Id = 1 });\n\
    var cmd = new ShipOrder();\n\
    await messageSession.Send(cmd);\n\
    await mediator.Publish(new LocalThing());\n\
    await http.SendAsync(new HttpRequestMessage());\n\
  }\n\
}\n\
public class SubmitConsumer : IConsumer<OrderSubmitted> {\n\
  public Task Consume(ConsumeContext<OrderSubmitted> c) => Task.CompletedTask;\n\
}\n\
public class ShipHandler : IHandleMessages<ShipOrder> {\n\
  public Task Handle(ShipOrder m, IMessageHandlerContext c) => Task.CompletedTask;\n\
}\n\
public class KafkaReader { IConsumer<string, byte[]> c; }\n";
    let mut x = CSharpExtractor::new().unwrap();
    assert_eq!(
        channel_edges(x.extract(src, "M").unwrap().edges),
        own(&[
            (
                "CHANNEL_PUBLISH",
                "Shop.Checkout.Go",
                "channel://ordersubmitted",
                "OrderSubmitted",
                "event-bus"
            ),
            (
                "CHANNEL_PUBLISH",
                "Shop.Checkout.Go",
                "channel://orderaccepted",
                "OrderAccepted",
                "event-bus"
            ),
            (
                "CHANNEL_PUBLISH",
                "Shop.Checkout.Go",
                "channel://shiporder",
                "ShipOrder",
                "event-bus"
            ),
            (
                "CHANNEL_SUBSCRIBE",
                "Shop.SubmitConsumer.Consume",
                "channel://ordersubmitted",
                "OrderSubmitted",
                "masstransit"
            ),
            (
                "CHANNEL_SUBSCRIBE",
                "Shop.ShipHandler.Handle",
                "channel://shiporder",
                "ShipOrder",
                "nservicebus"
            ),
        ])
    );
}

#[test]
fn a_kafka_consumer_subscribes_to_its_topic_default() {
    let src = "namespace Acc;\n\
class Consumer {\n\
  private static readonly string TopicName = Environment.GetEnvironmentVariable(\"KAFKA_TOPIC\") ?? \"orders\";\n\
  private IConsumer<string, byte[]> _consumer;\n\
  public Consumer() { _consumer.Subscribe(TopicName); }\n\
}\n";
    let mut x = CSharpExtractor::new().unwrap();
    assert_eq!(
        channel_edges(x.extract(src, "M").unwrap().edges),
        own(&[(
            "CHANNEL_SUBSCRIBE",
            "Acc.Consumer..ctor",
            "channel://orders",
            "orders",
            "azure-service-bus"
        )])
    );
}

#[test]
fn rabbitmq_clients_name_the_routing_key_and_queue() {
    let py = "class Publisher:\n    ROUTING_KEY = 'orders'\n\n    def _publish(self, msg):\n        self._channel.basic_publish(exchange='shop', routing_key=self.ROUTING_KEY, body=msg)\n\n    def listen(self):\n        self._channel.basic_consume(queue='orders', on_message_callback=None)\n";
    let mut x = PythonExtractor::new().unwrap();
    assert_eq!(
        channel_edges(x.extract(py, "pay").unwrap().edges),
        own(&[
            (
                "CHANNEL_PUBLISH",
                "pay.Publisher._publish",
                "channel://orders",
                "orders",
                "rabbitmq"
            ),
            (
                "CHANNEL_SUBSCRIBE",
                "pay.Publisher.listen",
                "channel://orders",
                "orders",
                "rabbitmq"
            ),
        ])
    );
    let go = "package main\n\nfunc main() {\n\tmsgs, _ := ch.Consume(\"orders\", \"\", true, false, false, false, nil)\n\t_ = msgs\n\tch.PublishWithContext(ctx, \"shop\", \"shipped\", false, false, m)\n}\n";
    let mut x = GoExtractor::new().unwrap();
    let edges: Vec<(String, String, String)> =
        channel_edges(x.extract(go, "dispatch").unwrap().edges)
            .into_iter()
            .map(|(k, _, key, name, _)| (k, key, name))
            .collect();
    assert_eq!(
        edges,
        vec![
            (
                "CHANNEL_SUBSCRIBE".to_string(),
                "channel://orders".to_string(),
                "orders".to_string()
            ),
            (
                "CHANNEL_PUBLISH".to_string(),
                "channel://shipped".to_string(),
                "shipped".to_string()
            ),
        ]
    );
}

#[test]
fn celery_tasks_are_received_by_name_and_sent_by_delay() {
    let src = "from celery import shared_task\n\n@app.task\ndef send_email(to):\n    pass\n\n@shared_task(name='proj.tasks.resize')\ndef resize_image(p):\n    pass\n\n@task\ndef deploy(c):\n    pass\n\ndef signup(user):\n    send_email.delay(user)\n    tasks.resize.apply_async((1,))\n    app.send_task('proj.tasks.cleanup')\n";
    let mut x = PythonExtractor::new().unwrap();
    assert_eq!(
        channel_edges(x.extract(src, "proj.tasks").unwrap().edges),
        own(&[
            (
                "CHANNEL_SUBSCRIBE",
                "proj.tasks.send_email",
                "channel://sendemail",
                "send_email",
                "celery"
            ),
            (
                "CHANNEL_SUBSCRIBE",
                "proj.tasks.resize_image",
                "channel://resize",
                "resize",
                "celery"
            ),
            (
                "CHANNEL_PUBLISH",
                "proj.tasks.signup",
                "channel://sendemail",
                "send_email",
                "celery"
            ),
            (
                "CHANNEL_PUBLISH",
                "proj.tasks.signup",
                "channel://resize",
                "resize",
                "celery"
            ),
            (
                "CHANNEL_PUBLISH",
                "proj.tasks.signup",
                "channel://cleanup",
                "cleanup",
                "celery"
            ),
        ])
    );
}
