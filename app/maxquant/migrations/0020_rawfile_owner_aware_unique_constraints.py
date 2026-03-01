from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("maxquant", "0019_alter_rawfile_unique_together"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="rawfile",
            unique_together=set(),
        ),
        migrations.AddConstraint(
            model_name="rawfile",
            constraint=models.UniqueConstraint(
                condition=models.Q(created_by__isnull=False),
                fields=("orig_file", "pipeline", "created_by"),
                name="rawfile_unique_owned_upload",
            ),
        ),
        migrations.AddConstraint(
            model_name="rawfile",
            constraint=models.UniqueConstraint(
                condition=models.Q(created_by__isnull=True),
                fields=("orig_file", "pipeline"),
                name="rawfile_unique_null_owner_upload",
            ),
        ),
    ]
