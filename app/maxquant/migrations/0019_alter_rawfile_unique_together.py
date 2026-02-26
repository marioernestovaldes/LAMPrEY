from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("maxquant", "0018_result_input_source"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="rawfile",
            unique_together={("orig_file", "pipeline", "created_by")},
        ),
    ]
