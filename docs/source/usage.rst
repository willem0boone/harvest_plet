Usage
==================

1. List available datasets
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    import harvest_plet
    from harvest_plet import list_datasets

    datasets = list_datasets.get_dataset_names()
    for i, name in enumerate(datasets):
        print(i, name)

2. Harvest individual dataset
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from datetime import date
    start = date(2010, 1, 1)
    end = date(2021, 1, 1)
    wkt_polygon = "POLYGON ((-180 -90,-180 90,180 90,180 -90,-180 -90))"
    dataset = "BE Flanders Marine Institute (VLIZ) - LW_VLIZ_zoo"

Option A: get result as string

.. code-block:: python

    from harvest_plet.harvest_dataset import harvest_dataset
    csv_data = harvest_dataset(start, end, wkt_polygon, dataset)
    print(csv_data)

Option B: get result as csv save

.. code-block:: python

    from harvest_plet.harvest_dataset import harvest_as_csv
    my_dir = ("C:/Users/willem.boone/Documents/projects/dto-bioflow/"
              "DUC4.3/hervest_results_single0")

    harvest_as_csv(start, end, wkt_polygon, dataset, out_dir=my_dir)


3. Harvest all datasets
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from harvest_plet.harvest_all import harvest_all_datasets
    my_dir = ("C:/Users/willem.boone/Documents/projects/dto-bioflow/"
              "DUC4.3/hervest_results_all")
    results, failures = harvest_all_datasets(out_dir=my_dir)