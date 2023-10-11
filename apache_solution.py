import apache_beam as beam
import typing
import logging

# Configure the logging settings
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


InvoiceSchema = typing.NamedTuple(
    "InvoiceSchema",
    legal_entity=str,
    counter_party=str,
    rating=int,
    status=str,
    value=int,
    tier=int,
)

beam.coders.registry.register_coder(InvoiceSchema, beam.coders.RowCoder)

ResultSchema = typing.NamedTuple(
    "ResultSchema",
    legal_entity=any,
    counter_party=any,
    tier=any,
    rating=int,
    ARAP=int,
    ACCR=int,
)


beam.coders.registry.register_coder(ResultSchema, beam.coders.RowCoder)


class Convert(beam.DoFn):
    """
    Combines and transforms elements.

    Args:
        element: The input element to process.
        type: An indicator of the type of processing to perform.

    Yields:
        ResultSchema: The result of processing.
    """

    def process(self, element, type):
        sumARAP = 0
        sumACCR = 0
        max_rating = 0
        for row in element[1]:
            if row.status == "ARAP":
                sumARAP += row.value
            else:
                sumACCR += row.value
            max_rating = max(max_rating, row.rating)

        yield ResultSchema(
            legal_entity=element[1][0].legal_entity
            if type == 1 or type == 2
            else "Total",
            counter_party=element[1][0].counter_party
            if type == 2 or type == 3
            else "Total",
            tier=element[1][0].tier if type == 4 else "Total",
            rating=max_rating,
            ACCR=sumACCR,
            ARAP=sumARAP,
        )


class JoinItem(beam.DoFn):
    def process(self, element, *args, **kwargs):
        tier = element[1][1][0]
        for item in element[1][0]:
            yield InvoiceSchema(
                legal_entity=item.legal_entity,
                counter_party=item.counter_party,
                rating=item.rating,
                status=item.status,
                value=item.value,
                tier=tier,
            )


with beam.Pipeline() as pipeline:
    logger.info("Data aggregation process started")
    logger.info("Reading and merging dataset files")
    # creating tranformation for reading csv datasets
    read_dataset1_tf = beam.io.ReadFromCsv("data/dataset1.csv", splittable=False)
    read_dataset1_tf.label = "read dataset1"
    read_dataset2_tf = beam.io.ReadFromCsv("data/dataset2.csv", splittable=False)
    read_dataset2_tf.label = "read dataset2"

    # Reading datasets
    dataset2 = pipeline | read_dataset2_tf
    dataset1 = pipeline | read_dataset1_tf | beam.Map(lambda x: (x.counter_party, x))

    # Joining datasets
    # InvoiceSchema(legal_entity='L1', counter_party='C1', rating=1, status='ARAP', value=10, tier=1)
    # InvoiceSchema(legal_entity='L1', counter_party='C1', rating=2, status='ARAP', value=10, tier=1)
    # ...
    joined_data = (
        [dataset1, dataset2]
        | "Group by counter_party" >> beam.CoGroupByKey()
        | "Join with dataset2"
        >> beam.ParDo(JoinItem()).with_output_types(InvoiceSchema)
    )

    # Grouping Rows by 'legal_entity' field
    # ResultSchema(legal_entity='L1', counter_party='Total', tier='Total', rating=6, ACCR=100, ARAP=85)
    # ResultSchema(legal_entity='L2', counter_party='Total', tier='Total', rating=6, ACCR=207, ARAP=1020)
    # ...
    legal_entity_group = (
        joined_data
        | "Grouping on the basis of legal entity" >> beam.GroupBy("legal_entity")
        | "converting resultset1"
        >> beam.ParDo(Convert(), 1).with_output_types(ResultSchema)
    )

    # Grouping Rows by ('counter_party','legal_entity') fields
    counter_legal_group = (
        joined_data
        | "Grouping on the basis of legal entity and counter party"
        >> beam.GroupBy("counter_party", "legal_entity")
        | "converting resultset2"
        >> beam.ParDo(Convert(), 2).with_output_types(ResultSchema)
    )

    # Grouping Rows by 'counter_party' field
    counter_party_group = (
        joined_data
        | "Grouping on the basis of counter party" >> beam.GroupBy("counter_party")
        | "converting resultset3"
        >> beam.ParDo(Convert(), 3).with_output_types(ResultSchema)
    )

    # Grouping Rows by 'tier' field
    tier_group = (
        joined_data
        | "Grouping on the basis of tier" >> beam.GroupBy("tier")
        | "converting resultset4"
        >> beam.ParDo(Convert(), 4).with_output_types(ResultSchema)
    )

    # Merging all result Pcollections to get final result
    merged_pcollection = (
        legal_entity_group,
        counter_legal_group,
        counter_party_group,
        tier_group,
    ) | "Merge PCollections" >> beam.Flatten()

    # Writing data to csv file
    (merged_pcollection | "Write to CSV " >> beam.io.WriteToCsv(path="output-beam"))

logger.info("Apache File Generated Successfully")
